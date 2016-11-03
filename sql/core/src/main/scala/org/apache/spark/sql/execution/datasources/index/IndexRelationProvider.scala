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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class IndexRelationProvider extends RelationProvider with CreatableRelationProvider
      with DataSourceRegister{

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {

    val tableName = parameters.getOrElse("path", sys.error("Index path isn't specified..."))
    IndexRelation(parameters, None, tableName, sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val tableName = parameters.getOrElse("path", sys.error("Index path isn't specified..."))

    val indexRelation: IndexRelation = new IndexRelation(
      parameters, Some(data), tableName, sqlContext.sparkSession)
    indexRelation.insert(data, overwrite = true)
    indexRelation
  }
  override def shortName(): String = "index"
}
