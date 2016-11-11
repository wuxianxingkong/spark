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

import org.apache.spark.internal.Logging
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.index.lucenerdd._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{FloatType, StructField, StructType}


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
    val midResult = filter match {
      case TermQuery(fieldName, query, topK) =>
        LuceneRDD(sparkSession, tableName).termQuery(
          fieldName, query, Integer.valueOf(topK))
      case FuzzyQuery(fieldName, query, maxEdits, topK) =>
        LuceneRDD(sparkSession, tableName).fuzzyQuery(
          fieldName, query, maxEdits, Integer.valueOf(topK))
      case PhraseQuery(fieldName, query, topK) =>
        LuceneRDD(sparkSession, tableName).phraseQuery(
          fieldName, query, Integer.valueOf(topK))
      case PrefixQuery(fieldName, query, topK) =>
        LuceneRDD(sparkSession, tableName).prefixQuery(
          fieldName, query, Integer.valueOf(topK))
      case ComplexQuery(query, topK) =>
        LuceneRDD(sparkSession, tableName).query(
          query, Integer.valueOf(topK))
      case _ => throw new
          UnsupportedOperationException(s"Cannot support other filter: $this")
    }

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
