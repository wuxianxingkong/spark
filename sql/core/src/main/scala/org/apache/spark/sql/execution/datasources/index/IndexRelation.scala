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

import scala.collection.JavaConversions._
import org.apache.lucene.index.IndexableField
import org.apache.spark.internal.Logging
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Descending, GenericInternalRow, GenericRowWithSchema, SortOrder, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{PartitionIdPassthrough, RDDConversions, ShuffledRowRDD, UnsafeRowSerializer}
import org.apache.spark.sql.execution.datasources.index.lucenerdd._
import org.apache.spark.sql.execution.datasources.index.lucenerdd.models.SparkScoreDoc
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{BoundedPriorityQueue, CompletionIterator, MutablePair, NextIterator}
import org.joda.time.DateTime



case class IndexRelation(
      val parameters: Map[String, String],
      val tableName: String,
      val sourceTable: String,
      val userSpecifiedSchema: StructType,
      @transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation with Logging{
  // val dataFrame: Option[DataFrame]
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  override def schema: StructType = {

    logInfo("Infer Schema from " +
      (if (show(parameters.get("quickway")).equals("yes")) "index" else "original table"))

    val finalSchema = parameters.get("quickway") match {
//      case Some("yes") =>
//        StructType(Seq(StructField("docId", IntegerType, true),
//          StructField("score", FloatType, true))
//          ++ sparkSession.table(sourceTable).schema.fields
//          ++ Seq(StructField("partitionIndex", IntegerType, true)))
//        // LuceneRDD.inferSchema(sparkSession, tableName)
      case _ =>
        StructType(Seq(StructField("score", FloatType, true))
          ++ sparkSession.table(sourceTable).schema.fields)
    }

    logInfo(s"Schema is $finalSchema")
    finalSchema
  }
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(s"User SpecifiedSchema: $userSpecifiedSchema")
    logInfo("Print filters..." + filters.map(filter => filter.toString).mkString(","))
    logInfo("Print requiredColumns..." + requiredColumns.mkString(","))
    // There is just on filter
    val filter = filters(0)
    var globalTopK = 0

    val startTime = new DateTime(System.currentTimeMillis())

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
    val executorSchema = StructType(
      Seq(StructField("docid", IntegerType, true)) ++ schema.fields)
    /* parameters.get("quickway") match {
        // If yes, just reuse schema, or, infer it
      case Some("yes") => schema
      case _ => LuceneRDD.inferSchema(sparkSession, tableName)
    } */

    val endTime = new DateTime(System.currentTimeMillis())
    logInfo(s"Search from indexes took ${(endTime.getMillis
      - startTime.getMillis) } milliseconds...")
    // solve bug:只选取需要的列
    // val prunedSchema = pruneSchema(executorSchema, requiredColumns)
    // Local topK
    val partitionResult = midResult.mapPartitions[InternalRow](iterator => {
        sparkScoreDocsToSparkInternalRows(iterator, executorSchema)
      }
    )

//    println("partitionResult:")
//    val tt = partitionResult
//    tt.foreach(println)
    val originalRDD = sparkSession.table(sourceTable).rdd
    // Do parallel connect with original data
    // println("quickway" + parameters.get("quickway"))
    val connectedData = parameters.get("quickway") match {
      // If yes, just reuse schema, or, infer it
      case Some("yes") =>
        partitionResult
      case _ =>
        val connectionStartTime = new DateTime(System.currentTimeMillis())
        // We use schema with score field(without docid)
        val extendedSchemaDTList = executorSchema.fields.map(_.dataType)
        // println("originalRDD_partition_length:" + originalRDD.partitions.length)
        val zipRDD = originalRDD.zipPartitions(partitionResult) {
          (rdd1Iterator, rdd2Iterator) => {
            var indexCount: Int = 0
            // Collect index needed to set
            val map = rdd2Iterator.toTraversable.map(row => (row.getInt(0) -> row.getFloat(1))).toMap
//            println("Collect index needed to set:")
//            map.foreach(println)
//            println("Value:")
//            val tempI = rdd1Iterator
//            tempI.zipWithIndex.foreach(println)
//            tempI.zipWithIndex.filter(pair =>
//              map.contains(pair._2)).foreach(println)
            val iterator = rdd1Iterator.zipWithIndex.filter(pair =>
              map.contains(pair._2)).map(pair => (pair._1, pair._2, map.get(pair._2)))
            val numColumns = extendedSchemaDTList.length
            // mutableRow会导致后面产生结果被复制两次的问题
            val converters = extendedSchemaDTList.map(
              CatalystTypeConverters.createToCatalystConverter)
            iterator.map { r =>
              val mutableRow = new GenericInternalRow(numColumns)
              mutableRow(0) = converters(0)(r._2)
              mutableRow(1) = converters(1)(r._3.get)
              var i = 2
              while (i < numColumns) {
                // In r._1(index) index must be i-1 because row data isn't start from index 0
                mutableRow(i) = converters(i)(r._1(i-2))
                i += 1
              }
              mutableRow
            }
          }
        }.asInstanceOf[RDD[InternalRow]] // [docid, score, [original_data_c1,original_data_c2, ...]]
        val connectionEndTime = new DateTime(System.currentTimeMillis())
        logInfo(s"Building Connection took ${(connectionEndTime.getMillis
          - connectionStartTime.getMillis) } milliseconds...")
        zipRDD
    }
    // only select necessary columns (+ score column)for shuffle
    val (prunedConnectedData, prunedSchema) = copyNecessaryColumns(
      Array("score") ++ requiredColumns,
      executorSchema, connectedData)
//    println("connectedData:")
//    val tt = connectedData
//    tt.foreach(println)
//    println("hihi")
//    connectedData.top(3)(descending).foreach(println)
    // Global topK
    val shuffled = new ShuffledRowRDD(
      prepareShuffleDependency(prunedConnectedData, SinglePartition))

    val finalResult_1 = shuffled.mapPartitions { iter =>
      val topK = org.apache.spark.util.collection.Utils.takeOrdered(
      iter.map(_.copy()), globalTopK)(descending(0))
      topK
    }
    val finalResult_2 = copyNecessaryColumns(
      requiredColumns,
      prunedSchema, finalResult_1)._1
//    println("size:"+shuffled.partitions.size)
//    shuffled.foreach(println)
//    partitionResult.asInstanceOf[RDD[Row]]
    finalResult_2.asInstanceOf[RDD[Row]]
  }
  def copyNecessaryColumns(array: Array[String],
    originalSchema: StructType,
    originalData: RDD[InternalRow]): (RDD[InternalRow], StructType) = {

    val fieldSet = (array).toSet
    var count = -1
    val fieldMap = originalSchema.fields.map(field => {
      count = count + 1
      (field.name, count, field)
    }).filter{ case(name, index, field) => fieldSet.contains(name)}
      .map{ case (name, count, field) =>
        name -> (count, field)
      }.toMap
    val fieldList: Array[(String, Int, StructField)] = array.map( p =>
      (p, fieldMap.get(p).get._1, fieldMap.get(p).get._2))
    val tt: Array[StructField] = fieldList.map{ case (name, index, field) => field}
    val prunedSchema = new StructType(tt)
    val prunedSchemaDTList = prunedSchema.fields.map(_.dataType)
    val prunedSchemaConverters = prunedSchemaDTList.map(
      CatalystTypeConverters.createToCatalystConverter)
    val prunedSchemaNumColumns = prunedSchemaDTList.length
    val prunedConnectedData = originalData.map(row => {
      val mutableRow = new GenericInternalRow(prunedSchemaNumColumns)
      var innerIndex = 0
      while (innerIndex < prunedSchemaNumColumns) {
        // In r._1(index) index must be i-1 because row data isn't start from index 0
        mutableRow(innerIndex) = prunedSchemaConverters(innerIndex)(
          row.get(fieldList(innerIndex)._2, prunedSchemaDTList(innerIndex)))
        innerIndex += 1
      }
      mutableRow
    })
    (prunedConnectedData.asInstanceOf[RDD[InternalRow]], prunedSchema)
  }
  /**
    * Ordering by score (descending)
    */
  def descending(compareIndex : Int): Ordering[InternalRow] = new Ordering[InternalRow]{
    // val compareIndex = if (show(parameters.get("quickway")).equals("yes")) 1 else 0
    // val compareIndex = 0
    override def compare(x: InternalRow, y: InternalRow): Int = {
      val left: Float = x.getFloat(compareIndex)
      val right: Float = y.getFloat(compareIndex)
      -left.compareTo(right)
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
        iter.map { row =>
          (part.getPartition(getPartitionKey(row)), row.copy()) }
      }
    }
//    println("hi")
//    rddWithPartitionIds.foreach(println)
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions))

    dependency
  }
  private type IndexValueGetter = (IndexableField, InternalRow, Int) => Unit

  private def makeGetters(schema: StructType): Array[IndexValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType))

  private def makeGetter(dt: DataType): IndexValueGetter = dt match {
    case IntegerType =>
      (field: IndexableField, row: InternalRow, pos: Int) =>
      row.setInt(pos, field.numericValue.intValue)
    case LongType =>
      (field: IndexableField, row: InternalRow, pos: Int) =>
        row.setLong(pos, field.numericValue.longValue)
    case DoubleType =>
      (field: IndexableField, row: InternalRow, pos: Int) =>
        row.setDouble(pos, field.numericValue.doubleValue)
    case FloatType =>
      (field: IndexableField, row: InternalRow, pos: Int) =>
        row.setFloat(pos, field.numericValue.floatValue)
    case StringType =>
      (field: IndexableField, row: InternalRow, pos: Int) =>
        row.update(pos, UTF8String.fromString(field.stringValue))
    case _ =>
      throw new IllegalArgumentException(s"Unsupported index type ${dt.simpleString}")
  }
  private def pruneSchema(schema: StructType, columns: Array[String]): (StructType,
    Map[String, StructField]) = {
    val columnsSet = columns.toSet
    val newColumns = if (columnsSet.contains("score"))  columns else Array("score") ++ columns
    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)

    (new StructType(newColumns.map(name => fieldMap(name))), fieldMap)
  }
  private[spark] def sparkScoreDocsToSparkInternalRows(
      iterator: Iterator[SparkScoreDoc],
      schema: StructType): Iterator[InternalRow] = {
    val rowsIterator = new NextIterator[InternalRow] {
      private[this] val iter = iterator
//      private[this] val (extendedSchema, fieldMap) = pruneSchema(schema, columns)
//      private[this] val extendedSchema = schema
      private[this] val extendedSchema = schema

      private[this] val getters: Array[IndexValueGetter] = makeGetters(extendedSchema)
      logInfo(s"extendedSchema: ${extendedSchema}")
      private[this] val mutableRow = new SpecificInternalRow(
        extendedSchema.fields.map(x => x.dataType))
      override protected def close(): Unit = {
        logInfo(s"SparkScoreDocsToSparkInternalRows finished at $iterator")
      }
      override protected def getNext(): InternalRow = {
        if (iter.hasNext) {
          val sparkScoreDoc = iter.next()
//          val fieldList = sparkScoreDoc.doc.doc.getFields.toList.filter(fieldMap.contains(_))
          val fieldList = sparkScoreDoc.doc.doc.getFields
//          // Get partitionIndex index
//          val partitionIndexFieldIndex = fieldList.map(
//            indexField => indexField.name()).indexOf("partitionIndex")
//          // Get partitionIndex
//          val partitionIndex = fieldList(partitionIndexFieldIndex).name
//          fieldList.remove(partitionIndexFieldIndex)
          // First three columns can't use 'getters'
          // Set docId column

          mutableRow.setInt(0, sparkScoreDoc.docId)
          // Set partitionIndex/shardIndex column
//          if (!show(parameters.get("quickway")).equals("yes")) {
//            mutableRow.setInt(1, partitionIndex.toInt)
//          } else {
//            mutableRow.setInt(1, sparkScoreDoc.shardIndex)
//          }
          // mutableRow.setInt(1, sparkScoreDoc.shardIndex)
          // Set score column
          mutableRow.setFloat(1, sparkScoreDoc.score)
          // Begin at index 3
          var i = 2
          // logInfo(s"length = ${getters.length}")
          while (i< getters.length && fieldList.size()>0 ) {
            // logInfo(s"i = $i")
            getters(i).apply(fieldList.get(i-2), mutableRow, i)
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
  def show(x: Option[String]): String = x match {
    case Some(s) => s
    case None => "?"
  }
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    sparkSession.sparkContext.objectFile("")
    // quickway == yes means store all column data
//    if (parameters.contains("quickway") && show(parameters.get("quickway")).equals("yes")) {
      val indexColumns_string = parameters.getOrElse("indexColumns",
        sys.error("Index path isn't specified..."))
      val indexColumns = indexColumns_string.split(",")
      logInfo(s"First, delete existing index path recursively: ${tableName}...")
      val startTime = new DateTime(System.currentTimeMillis())
      val rdd = LuceneRDD(data, tableName, indexColumns.toSeq,
        show(parameters.get("quickway")).equals("yes"))
      val midTime = new DateTime(System.currentTimeMillis())
      logInfo(s"LuceneRDD construction took ${(midTime.getMillis
      - startTime.getMillis) / 1000} seconds...")
      rdd.count()
      val endTime = new DateTime(System.currentTimeMillis())
      logInfo(s"Building indexes took ${(endTime.getMillis
      - startTime.getMillis) / 1000} seconds...")
//    } else {
//      // quickway == false means
//      // needs connection between original rdd and result rdd
//      val rdd = LuceneRDD(data, tableName)
//      rdd.count()
//    }

    //

  }
}
