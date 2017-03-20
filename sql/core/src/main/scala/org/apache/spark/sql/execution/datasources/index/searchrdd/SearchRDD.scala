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

package org.apache.spark.sql.execution.datasources.index.searchrdd

import com.twitter.algebird.{TopK, TopKMonoid}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.lucene.document._
import org.apache.lucene.index.DirectoryReader
import org.apache.spark.sql.execution.datasources.index.searchrdd.config.LuceneRDDConfigurable
import org.apache.spark.sql.execution.datasources.index.searchrdd.response.{LuceneRDDResponse, LuceneRDDResponsePartition}
import org.apache.spark.rdd.RDD
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query}
import org.apache.solr.store.hdfs.HdfsDirectory
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.execution.datasources.index.searchrdd.partition.{AbstractLuceneRDDPartition, LuceneRDDPartition}
import org.apache.spark.sql.execution.datasources.index.searchrdd.models.SparkScoreDoc
import org.apache.spark.sql.execution.datasources.index.searchrdd.store.Status
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Spark RDD with Lucene's query capabilities (term, prefix, fuzzy, phrase query)
 *
 * @tparam T
 */
class SearchRDD[T: ClassTag](
      protected val partitionsRDD: RDD[AbstractLuceneRDDPartition[T]])
      extends RDD[T](partitionsRDD.sparkContext,
      if ( partitionsRDD ==null ) Nil else List(new OneToOneDependency(partitionsRDD)))
  with LuceneRDDConfigurable {

  logInfo("Instance is created...")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def cache(): SearchRDD.this.type = {
    this.persist(StorageLevel.MEMORY_ONLY)
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    super.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    super.unpersist(blocking)
    this
  }

  /** Set the name for the RDD; By default set to "LuceneRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("LuceneRDD")

  /**
   * Maps partition results
   *
   * @param f Function to apply on each partition / distributed index
   * @param k number of documents to return
   * @return
   */
  protected def partitionMapper(f: AbstractLuceneRDDPartition[T] => LuceneRDDResponsePartition,
                                k: Int): LuceneRDDResponse = {
    new LuceneRDDResponse(partitionsRDD.map(f), SparkScoreDoc.descending)
  }


  /**
   * Return all document fields
   *
   * @return
   */
  def fields(): Set[String] = {
    logInfo("Fields requested")
    partitionsRDD.map(_.fields()).reduce(_ ++ _)
  }

  /**
   * Lucene generic query
   *
   * @param doc
   * @return
   */
  def exists(doc: Map[String, String]): Boolean = {
    !partitionMapper(_.multiTermQuery(doc, DefaultTopK), DefaultTopK).isEmpty()
  }

  /**
   * Generic query using Lucene's query parser
   * @param defaultField  Default query field
   * @param searchString  Query String
   * @param topK
   * @return
   */
  def query(defaultField: String, searchString: String,
            topK: Int = DefaultTopK): LuceneRDDResponse = {
    partitionMapper(_.query(defaultField, searchString, topK), topK)
  }


  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other DataFrame to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @param topK
   * @return an RDD of Tuple2 that contains the linked search Lucene documents in the second
   */
  def linkDataFrame(other: DataFrame, defaultField: String,
        searchQueryGen: Row => String, topK: Int = DefaultTopK)
  : RDD[(Row, List[SparkScoreDoc])] = {
    logInfo("LinkDataFrame requested")
    link[Row](other.rdd, defaultField, searchQueryGen, topK)
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other RDD to be linked
   * @param searchQueryGen Function that generates a search query for each element of other
   * @tparam T1 A type
   * @return an RDD of Tuple2 that contains the linked search Lucene documents in the second
   *
   * Note: Currently the query strings of the other RDD are collected to the driver and
   * broadcast to the workers.
   */
  def link[T1: ClassTag](other: RDD[T1], defaultField: String,
        searchQueryGen: T1 => String, topK: Int = DefaultTopK)
    : RDD[(T1, List[SparkScoreDoc])] = {
    logInfo("Linkage requested")
    val monoid = new TopKMonoid[SparkScoreDoc](topK)(SparkScoreDoc.descending)
    logDebug("Collecting query points to driver")
    val queries = other.map(searchQueryGen).collect()
    logDebug("Query points collected to driver successfully")
    logDebug("Broadcasting query points")
    val queriesB = partitionsRDD.context.broadcast(queries)
    logDebug("Query points broadcasting was successfully")

    val resultsByPart: RDD[(Long, TopK[SparkScoreDoc])] = partitionsRDD.flatMap {
      case partition => queriesB.value.zipWithIndex.map { case (qr, index) =>
        val results = partition.query(defaultField, qr, topK)
          .map(x => monoid.build(x))

        (index.toLong, results.reduceOption(monoid.plus)
          .getOrElse(monoid.zero))
      }
    }

    logDebug("Compute topK linkage per partition")
    val results = resultsByPart.reduceByKey(monoid.plus)

    //  Asynchronously delete cached copies of this broadcast on the executors
    queriesB.unpersist()

    other.zipWithIndex.map(_.swap).join(results).values
      .map(joined => (joined._1, joined._2.items.take(topK)))
  }

  /**
   * Entity linkage via Lucene query over all elements of an RDD.
   *
   * @param other RDD to be linked
   * @param searchQueryGen Function that generates a Lucene Query object for each element of other
   * @tparam T1 A type
   * @return an RDD of Tuple2 that contains the linked search Lucene Document in the second position
   */
  def linkByQuery[T1: ClassTag](other: RDD[T1], defaultField: String,
                                searchQueryGen: T1 => Query, topK: Int = DefaultTopK)
  : RDD[(T1, List[SparkScoreDoc])] = {
    logInfo("LinkByQuery requested")
    def typeToQueryString = (input: T1) => {
      searchQueryGen(input).toString
    }

    link[T1](other, defaultField, typeToQueryString, topK)
  }

  /**
   * Lucene term query
   *
   * @param fieldName Name of field
   * @param query Term to search on
   * @param topK Number of documents to return
   * @return
   */
  def termQuery(fieldName: String, query: String,
                topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Term search on field ${fieldName} with query ${query}")
    partitionMapper(_.termQuery(fieldName, query, topK), topK)
  }

  /**
   * Lucene prefix query
   *
   * @param fieldName Name of field
   * @param query Prefix query text
   * @param topK Number of documents to return
   * @return
   */
  def prefixQuery(fieldName: String, query: String,
                  topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Prefix search on field ${fieldName} with query ${query}")
    partitionMapper(_.prefixQuery(fieldName, query, topK), topK)
  }

  /**
   * Lucene fuzzy query
   *
   * @param fieldName Name of field
   * @param query Query text
   * @param maxEdits Fuzziness, edit distance
   * @param topK Number of documents to return
   * @return
   */
  def fuzzyQuery(fieldName: String, query: String,
                 maxEdits: Int, topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Fuzzy search on field ${fieldName} with query ${query}")
    partitionMapper(_.fuzzyQuery(fieldName, query, maxEdits, topK), topK)
  }

  /**
   * Lucene phrase Query
   *
   * @param fieldName Name of field
   * @param query Query text
   * @param topK Number of documents to return
   * @return
   */
  def phraseQuery(fieldName: String, query: String,
                  topK: Int = DefaultTopK): LuceneRDDResponse = {
    logInfo(s"Phrase search on field ${fieldName} with query ${query}")
    partitionMapper(_.phraseQuery(fieldName, query, topK), topK)
  }

//  override def count(): Long = {
//    logInfo("Count action requested")
//    partitionsRDD.map(_.size).reduce(_ + _)
//  }

  /** RDD compute method. */
  override def compute(part: Partition, context: TaskContext): Iterator[T] = {
    firstParent[AbstractLuceneRDDPartition[T]].iterator(part, context).next.iterator
  }

  override def filter(pred: T => Boolean): SearchRDD[T] = {
    val newPartitionRDD = partitionsRDD.mapPartitions(partition =>
      partition.map(_.filter(pred)), preservesPartitioning = true
    )
    new SearchRDD(newPartitionRDD)
  }

  /**
   * After Index and remove current RDD
   * We can't find anything from original data
   * So we abandon this method
   */
//  def exists(elem: T): Boolean = {
//    partitionsRDD.map(_.isDefined(elem)).collect().exists(x => x)
//  }

  def close(): Unit = {
    logInfo("Closing LuceneRDD...")
    partitionsRDD.foreach(_.close())
  }
}

object SearchRDD{

  /**
   * Instantiate a LuceneRDD given an RDD[T]
   *
   * @param elems RDD of type T
   * @tparam T Generic type
   * @return
   */
  def apply[T : ClassTag](conf: Configuration, elems: RDD[T], tableName: String)
    (implicit conv: T => Document): SearchRDD[T] = {
    val serialConf = new SeriConfiguration(conf)
    serialConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val deletePath = new Path(tableName)
    val deleteDf = FileSystem.get(conf)
    if(deleteDf.exists(deletePath)) {
      deleteDf.delete(deletePath, true)
    }
    val partitions = elems.mapPartitions[AbstractLuceneRDDPartition[T]](
      iter => Iterator(LuceneRDDPartition(iter, serialConf, tableName, Status.Rewrite)),
      preservesPartitioning = true)
    new SearchRDD[T](partitions)
  }

  /**
    * Instantiate a LuceneRDD given an RDD[Row] and indexed columns
    *
    * @param elems RDD of type Row
    * @param tableName table name
    * @param indexColumns columns to be indexed
    * @return
    */
  def apply(conf: Configuration, elems: RDD[Row],
      tableName: String, indexColumns: Seq[String], quickWay: Boolean)
      : SearchRDD[Row] = {
    val serialConf = new SeriConfiguration(conf)
    serialConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val deletePath = new Path(tableName)
    val deleteDf = FileSystem.get(conf)
    if(deleteDf.exists(deletePath)) {
      deleteDf.delete(deletePath, true)
    }
    val partitions = elems.mapPartitionsWithIndex[AbstractLuceneRDDPartition[Row]](
      (index, iterator) =>
        Iterator(LuceneRDDPartition(indexColumns, quickWay: Boolean, index, iterator, serialConf,
        tableName, Status.Rewrite)),
      preservesPartitioning = true)
    new SearchRDD[Row](partitions)
  }

  /**
    * Instantiate a LuceneRDD with existing index paths
    *
    * @param tableName Existing index path
    * @return
    */
  def apply(sparkSession: SparkSession, tableName: String)
                         (implicit conv: String => Document): SearchRDD[String] = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(tableName)
    val hdfs = FileSystem.get(conf)
    // Find all index partitions from HDFS
    if(hdfs.exists(hdfsPath)) {
      val qualified = hdfsPath.makeQualified(hdfs.getUri, hdfs.getWorkingDirectory)
      val directorys = hdfs.listStatus(qualified)
      val indexPathsFiltered = directorys.filter(directory =>
        directory.isDirectory && directory.getPath.toString.contains("indexDirectory"))
      // Sort by partitionIndex asc
      val indexPaths = indexPathsFiltered.map(directory =>
        directory.getPath.toString).sortWith(
        (s, t) => s.substring(s.indexOf("indexDirectory") + 15,
          s.indexOf(".", s.indexOf("indexDirectory") + 15)).toInt.
          compareTo(t.substring(t.indexOf("indexDirectory") + 15,
            t.indexOf(".", t.indexOf("indexDirectory") + 15)).toInt) < 0)
      val serialConf = new SeriConfiguration(conf)
      serialConf.setBoolean("fs.hdfs.impl.disable.cache", true);
      val rdd = sparkSession.sparkContext.parallelize[String](
        indexPaths.toList.asInstanceOf[Seq[String]], indexPaths.toSeq.size)
      val partitions = rdd.mapPartitionsWithIndex[AbstractLuceneRDDPartition[String]](
        (index, iterator) => {
          val temp = iterator.take(1).next().asInstanceOf[String]
          Iterator(LuceneRDDPartition(iterator, serialConf, temp, Status.Exists))},
        preservesPartitioning = true)
      return new SearchRDD[String](partitions)
    } else {
      sys.error(s"${tableName} isn't exist")
    }
    null
  }

  /**
   * Instantiate a LuceneRDD with an iterable
   *
   * @param elems Elements to index
   * @tparam T Input type
   * @return
   */
  def apply[T : ClassTag]
  (sc: SparkContext,
   elems: Iterable[T], tableName: String)
  (implicit conv: T => Document)
  : SearchRDD[T] = {
    apply(sc.hadoopConfiguration, sc.parallelize[T](elems.toSeq), tableName)
  }

  /**
   * Instantiate a LuceneRDD with DataFrame
   *
   * @param dataFrame Spark DataFrame
   * @param tableName tableName
   * @return
   */
  def apply(dataFrame: DataFrame, tableName: String)
  : SearchRDD[Row] = {
    apply(dataFrame.sqlContext.sparkContext.hadoopConfiguration, dataFrame.rdd, tableName)
  }

  /**
    * Instantiate a LuceneRDD with DataFrame
    *
    * @param dataFrame Spark DataFrame
    * @param tableName tableName
    * @param indexColumns: Seq[String]
    * @return
    */
  def apply(dataFrame: DataFrame, tableName: String, indexColumns: Seq[String], quickWay: Boolean)
  : SearchRDD[Row] = {
    apply(dataFrame.sqlContext.sparkContext.hadoopConfiguration,
      dataFrame.rdd, tableName, indexColumns, quickWay)
  }

  /**
   * Infer schema
   * @param sparkSession SparkSession
   * @param tableName tableName
   * @return
   */
  def inferSchema(sparkSession: SparkSession, tableName: String): StructType = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(tableName)
    val hdfs = FileSystem.get(conf)
    // Find all index partitions from HDFS
    if(hdfs.exists(hdfsPath)) {
      val qualified = hdfsPath.makeQualified(hdfs.getUri, hdfs.getWorkingDirectory)
      val directorys = hdfs.listStatus(qualified)
      val fullPath = directorys.filter(directory =>
        directory.isDirectory && directory.getPath.toString.contains("indexDirectory")).head.getPath
      val directory = new HdfsDirectory(fullPath, conf)
      val indexReader = DirectoryReader.open(directory)
      val indexSearcher = new IndexSearcher(indexReader)
      val topDocs = indexSearcher.search(new MatchAllDocsQuery(), 1).scoreDocs
      val allFields = topDocs.flatMap(x =>
        indexSearcher.getIndexReader.document(x.doc).getFields.asScala
      ).map(field =>
        if (field.numericValue() != null) {
          val number = field.numericValue()
//          val intv = number.intValue() + ""
//          val intl = number.longValue() + ""
//          val intf = number.floatValue() + ""
//          val intd = number.doubleValue() + ""
          number match {
            case a: Integer => StructField(field.name, IntegerType, true)
            case b: java.lang.Long => StructField(field.name, LongType, true)
            case c: java.lang.Float => StructField(field.name, FloatType, true)
            case d: java.lang.Double => StructField(field.name, DoubleType, true)
          }
        } else {
          StructField(field.name, StringType, true)
        }
//        field match {
//        case a: IntField => StructField(a.name, IntegerType, true)
//        case b: LongField => StructField(b.name, LongType, true)
//        case c: FloatField => StructField(c.name, FloatType, true)
//        case d: DoubleField => StructField(d.name, DoubleType, true)
//        case e: TextField => StructField(e.name, StringType, true)
//        case _ => StructField(field.name, StringType, true)}
      )
      // StructField("shardIndex", IntegerType, true),
      val originSchema = new StructType(allFields)
      StructType(Seq(StructField("docId", IntegerType, true),
        StructField("score", FloatType, true)) ++ originSchema.fields)
    } else {
      sys.error(s"${tableName} isn't exist")
      new StructType(Array[StructField]())
    }
  }
  /**
   * Return project information, i.e., version number, build time etc
   * @return
   */
  def version(): Map[String, Any] = {
    // BuildInfo is automatically generated using sbt plugin `sbt-buildinfo`
    // org.apache.spark.sql.execution.datasources
    // index.org.zouzias.spark.lucenerdd.justtemp.BuildInfo.toMap
    null
  }

}
