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
      val dataFrame: Option[DataFrame],
      val tableName: String,
      @transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    
    LuceneRDD.inferSchema(sparkSession, tableName)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    LuceneRDD(sparkSession, tableName).termQuery("", "", 10)
    null
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val rdd = LuceneRDD(data, tableName)
    rdd.count()
  }
}
