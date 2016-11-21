/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.index

import java.util.Properties

import org.apache.lucene.index.IndexableField
import org.apache.spark.internal.Logging
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Descending, MutableRow, SortOrder, SpecificMutableRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{PartitionIdPassthrough, ShuffledRowRDD, UnsafeRowSerializer}
import org.apache.spark.sql.execution.datasources.index.lucenerdd._
import org.apache.spark.sql.execution.datasources.index.lucenerdd.models.SparkScoreDoc
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{BoundedPriorityQueue, CompletionIterator, MutablePair, NextIterator}


case class IndexRelation(
      val parameters: Map[String, String],
      val tableName: String,
      val userSpecifiedSchema: StructType,
      @transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation with Logging{
  // val dataFrame: Option[DataFrame]
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  override def schema: StructType = {
    logInfo("Infer Schema from index...")
    val struct = LuceneRDD.inferSchema(sparkSession, tableName)
    logInfo(s"Schema is $struct")
    struct
  }
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(s"User SpecifiedSchema: $userSpecifiedSchema")
    logInfo("Print filters..." + filters.map(filter => filter.toString).mkString(","))
    // There is just on filter
    val filter = filters(0)
    var globalTopK = 0
    val midResult = filter match {
      case TermQuery(fieldName, query, topK) =>
        globalTopK = topK
        LuceneRDD(sparkSession, tableName).termQuery(
          fieldName, query, Integer.valueOf(topK))
      case FuzzyQuery(fieldName, query, maxEdits, topK) =>
        globalTopK = topK
        LuceneRDD(sparkSession, tableName).fuzzyQuery(
          fieldName, query, maxEdits, Integer.valueOf(topK))
      case PhraseQuery(fieldName, query, topK) =>
        globalTopK = topK
        LuceneRDD(sparkSession, tableName).phraseQuery(
          fieldName, query, Integer.valueOf(topK))
      case PrefixQuery(fieldName, query, topK) =>
        globalTopK = topK
        LuceneRDD(sparkSession, tableName).prefixQuery(
          fieldName, query, Integer.valueOf(topK))
      case QueryParser(defaultFieldName, query, topK) =>
        globalTopK = topK
        LuceneRDD(sparkSession, tableName).query(
          defaultFieldName, query, Integer.valueOf(topK))
      case _ => throw new
          UnsupportedOperationException(s"Cannot support other filter: $this")
    }
    // We can't just pass schema to mapPartitions function because schema
    // will lose sparkSession value when executed in executor.
    // So, we save schema to executorSchema
    val executorSchema = schema
    // Local topK
    val partitionResult = midResult.mapPartitions[InternalRow](iterator =>
      sparkScoreDocsToSparkInternalRows(iterator, executorSchema)
    )
    // Global topK
    val shuffled = new ShuffledRowRDD(
      prepareShuffleDependency(partitionResult, SinglePartition))
    val finalresult = shuffled.mapPartitions { iter =>
      val topK = org.apache.spark.util.collection.Utils.takeOrdered(
      iter.map(_.copy()), globalTopK)(descending)
      topK
    }
//    println("size:"+shuffled.partitions.size)
//    shuffled.foreach(println)
//    partitionResult.asInstanceOf[RDD[Row]]
    finalresult.asInstanceOf[RDD[Row]]
  }
  /**
    * Ordering by score (descending)
    */
  def descending: Ordering[InternalRow] = new Ordering[InternalRow]{
    override def compare(x: InternalRow, y: InternalRow): Int = {
      val left: Float = x.getFloat(2)
      val right: Float = y.getFloat(2)
      -left.compareTo(right)
    }
  }

  private def needToCopyObjectsBeforeShuffle(
                                              partitioner: Partitioner,
                                              serializer: Serializer): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    if (sortBasedShuffleOn) {
      val bypassIsSupported = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
      if (bypassIsSupported && partitioner.numPartitions <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (serializer.supportsRelocationOfSerializedObjects) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties. If this optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, so we only
        // need to check whether the optimization is enabled and supported by our serializer.
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        true
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }
  def prepareShuffleDependency(rdd: RDD[InternalRow],
        newPartitioning: Partitioning): ShuffleDependency[Int, InternalRow, InternalRow] = {
//    val serializer: Serializer = new UnsafeRowSerializer(onceSchema.fields.size)
    val part: Partitioner = new Partitioner {
      override def numPartitions: Int = 1
      override def getPartition(key: Any): Int = 0
    }
    def getPartitionKeyExtractor(): InternalRow => Any = identity
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      rdd.mapPartitionsInternal { iter =>
        val getPartitionKey = getPartitionKeyExtractor()
        iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
      }
    }
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions))

    dependency
  }
  private type IndexValueGetter = (IndexableField, MutableRow, Int) => Unit

  private def makeGetters(schema: StructType): Array[IndexValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType))

  private def makeGetter(dt: DataType): IndexValueGetter = dt match {
    case IntegerType =>
      (field: IndexableField, row: MutableRow, pos: Int) =>
      row.setInt(pos, field.numericValue.intValue)
    case LongType =>
      (field: IndexableField, row: MutableRow, pos: Int) =>
        row.setLong(pos, field.numericValue.longValue)
    case DoubleType =>
      (field: IndexableField, row: MutableRow, pos: Int) =>
        row.setDouble(pos, field.numericValue.doubleValue)
    case FloatType =>
      (field: IndexableField, row: MutableRow, pos: Int) =>
        row.setFloat(pos, field.numericValue.floatValue)
    case StringType =>
      (field: IndexableField, row: MutableRow, pos: Int) =>
        row.update(pos, UTF8String.fromString(field.stringValue))
    case _ =>
      throw new IllegalArgumentException(s"Unsupported index type ${dt.simpleString}")
  }
  private[spark] def sparkScoreDocsToSparkInternalRows(
      iterator: Iterator[SparkScoreDoc],
      schema: StructType): Iterator[InternalRow] = {
    val rowsIterator = new NextIterator[InternalRow] {
      private[this] val iter = iterator
      private[this] val extendedSchema = schema
//      private[this] val extendedSchema =
//        StructType(Seq(StructField("docId", IntegerType, true),
//          StructField("shardIndex", IntegerType, true),
//          StructField("score", FloatType, true)) ++ schema.fields)

      private[this] val getters: Array[IndexValueGetter] = makeGetters(extendedSchema)
      logInfo(s"extendedSchema: ${extendedSchema}")
      private[this] val mutableRow = new SpecificMutableRow(
        extendedSchema.fields.map(x => x.dataType))
      override protected def close(): Unit = {
        logInfo(s"SparkScoreDocsToSparkInternalRows finished at $iterator")
      }
      override protected def getNext(): InternalRow = {
        if (iter.hasNext) {
          val sparkScoreDoc = iter.next()
          val fieldList = sparkScoreDoc.doc.doc.getFields()
          // First three columns can't use 'getters'
          // Set docId column
          mutableRow.setInt(0, sparkScoreDoc.docId)
          // Set shardIndex column
          mutableRow.setInt(1, sparkScoreDoc.shardIndex)
          // Set score column
          mutableRow.setFloat(2, sparkScoreDoc.score)
          // Begin at index 3
          var i = 3
          // logInfo(s"length = ${getters.length}")
          while (i< getters.length) {
            logInfo(s"i = $i")
            getters(i).apply(fieldList.get(i-3), mutableRow, i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
    def close() {
      logInfo("Finish this partition.")
    }
    CompletionIterator[InternalRow, Iterator[InternalRow]](rowsIterator, close())
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }
    if (parameters.contains("quickway") && show(parameters.get("quickway")).equals("yes")) {
      val indexColumns_string = parameters.getOrElse("indexColumns",
        sys.error("Index path isn't specified..."))
      val indexColumns = indexColumns_string.split(",")
      val rdd = LuceneRDD(data, tableName, indexColumns.toSeq)
      rdd.count()
    } else {
      val rdd = LuceneRDD(data, tableName)
      rdd.count()
    }

    //

  }
}
