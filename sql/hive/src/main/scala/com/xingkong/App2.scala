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

// scalastyle:off
package com.xingkong

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object App2 {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().master("local").appName("Test1").
      config("spark.sql.catalogImplementation","hive").
      config("spark.eventLog.enabled", true).
      config("spark.eventLog.dir", "sql/hive/spark-events").getOrCreate()
    val df = sparkSession.read.json("sql/hive/source2")
    //    //df.show()
    df.createOrReplaceTempView("tbl_b")
//    val df1=sparkSession.sql("SELECT * INTO NEW_TABLE FROM OLD_TABLE")
//    df1.explain(true)
//    df1.show()
    //    df1.explain()
    //    df1.show()
    //    val df1=sparkSession.sql("SELECT * INTO NEW_TABLE FROM source2")
    //    val df2=sparkSession.sql("CREATE TABLE IF NOT EXISTS NEW_TABLE (KEY INT, VALUE STRING)")
    //    val df1=sparkSession.sql("SELECT PRODUCT_ID,PRODUCT_TYPE_ID INTO NEW_TABLE FROM source2")
    //    df1.explain(true)
    //    df1.show()
    sparkSession.sql("SET spark.sql.shuffle.partitions=1")
//    sparkSession.sql("SELECT description,name FROM OLD_TABLE").show()
//    println("-----------------------------order by--------------------------------------")
//    println("*************select into*************")
//    sparkSession.sql("DROP TABLE  IF EXISTS NEW_TABLE")
//    val df3 = sparkSession.sql("SELECT description as a1,name as a2 INTO NEW_TABLE FROM OLD_TABLE ORDER BY name")
//    df3.explain(true)
//    sparkSession.sql("SELECT * FROM NEW_TABLE").show()
//    sparkSession.sql("DROP TABLE  IF EXISTS NEW_TABLE_test")
//    sparkSession.sql("SELECT description as a1,name as a2 INTO NEW_TABLE_test FROM OLD_TABLE ORDER BY a1 DESC")
//    sparkSession.sql("SELECT * FROM NEW_TABLE_test").show()
//    sparkSession.sql("SELECT description,name FROM OLD_TABLE ORDER BY name").show()
//    println("*************CTAS************")
//    sparkSession.sql("DROP TABLE  IF EXISTS NEW_TABLE_1")
//    val df4 = sparkSession.sql("CREATE TABLE NEW_TABLE_1 AS SELECT description,name FROM OLD_TABLE ORDER BY name")
//    df4.explain(true)
//    sparkSession.sql("SELECT description,name FROM NEW_TABLE_1").show()
//    println("-------------------------------JOIN-----------------------------------")
//    println("*************select into*************")
//    sparkSession.sql("DROP TABLE  IF EXISTS NEW_TABLE_2")
//    sparkSession.sql("SELECT A.description,A.name INTO NEW_TABLE_2 FROM OLD_TABLE AS A JOIN OLD_TABLE AS B")
//    sparkSession.sql("SELECT * FROM NEW_TABLE_2").show()
//    println("*************CTAS************")
//    sparkSession.sql("DROP TABLE  IF EXISTS NEW_TABLE_3")
//    sparkSession.sql("CREATE TABLE NEW_TABLE_3 AS SELECT A.description,A.name FROM OLD_TABLE AS A JOIN OLD_TABLE AS B")
//    sparkSession.sql("SELECT * FROM NEW_TABLE_3").show()
//      println("测试未通过语句")
//    sparkSession.sql("DROP TABLE IF EXISTS src")
//    sparkSession.sql("CREATE TABLE src (key STRING, value STRING) STORED AS TEXTFILE")
//    sparkSession.sql("LOAD DATA LOCAL INPATH \"/home/cuiguangfan/projects_workspaces/intellij_idea_workspace_0/spark/sql/hive/src/test/resources/data/files/kv1.txt\" INTO TABLE src")
//    sparkSession.sql("select count(*) from src").show()
//    sparkSession.sql("select * from src").show()
//    sparkSession.sql("from src select transform('aa\\;') using 'cat' as a  limit 1").show()
//    sparkSession.sql("select count(*) from (from src select transform('aa\\;') using 'cat' as a  limit 1)").show()
//      sparkSession.sql("select * from old_table limit 1").show()
//    println("test multi select query")
//    sparkSession.sql("create table tbl1 as select * from OLD_TABLE limit 0")
//    sparkSession.sql("create table tbl1 as select * from OLD_TABLE limit 0")
//    sparkSession.sql("from OLD_TABLE insert into tbl1 select * insert into tbl2 select * where s < 10")
      /*println("test 1")
      sparkSession.sql("drop table if exists new_table")
      sparkSession.sql("select 1  into new_table from old_table")
      sparkSession.sql("select * from new_table").show()
    println("test 2")
    sparkSession.sql("drop table if exists new_table")
    sparkSession.sql("select 1 into new_table from old_table")
    sparkSession.sql("select * from new_table").show()*/
    /*println("test 3")
    sparkSession.sql("drop table if exists new_table")
    sparkSession.sql("select 1 into new_table")
    sparkSession.sql("select * from new_table").show()*/
    /*println("test 4")
    sparkSession.sql("drop table if exists new_table")
    sparkSession.sql("insert into new_table select * into new_table1 from old_table")
    sparkSession.sql("select * from new_table").show()*/
    /*println("test 5")
    sparkSession.sql("drop table if exists new_table1")
    sparkSession.sql("drop table if exists new_table2")
    sparkSession.sql("from old_table insert into new_table1 select * insert into new_table2 select * into new_table3 ")
    sparkSession.sql("select * from new_table").show()*/
//    sparkSession.sql("drop table if exists new_table1")
//    sparkSession.sql("SELECT PRODUCT_ID INTO new_table1 FROM OLD_TABLE")
//    sparkSession.sql("SELECT * FROM new_table1").show()
    //sparkSession.sql("SELECT 1 FROM OLD_TABLE").show()
    //println("test 6")
//    sparkSession.sql("drop table if exists new_table1")
//    sparkSession.sql("create table new_talbe1 as select 1")
//    sparkSession.sql("select * from new_table1").show()
    //val df_test6=sparkSession.sql("select 1")
    //df_test6.explain()
    //df_test6.show()
//    sparkSession.sql("drop table if exists t1")
//    val df_test7=sparkSession.sql("create table t1 as select 1")
//    sparkSession.sql("select * from t1").show()
    sparkSession.sql("drop table if exists tbl_a")
    val df1=sparkSession.sql("SELECT PRODUCT_ID INTO tbl_a FROM tbl_b")
    df1.explain(true)
    //    val df3=sparkSession.sql("INSERT INTO OLD_TABLE VALUES ('hi','ha',1.1,1,2)")
    //    df3.explain(true)
    //    df3.show()
    //    val df5=sparkSession.sql("SELECT * FROM NEW_TABLE")
    //    df5.explain(true)
    //    df5.show()
    //    val df4=sparkSession.sql("SELECT * FROM OLD_TABLE")
    //    df4.explain(true)
    //    df4.show()
//          val df4=sparkSession.sql("CREATE TABLE IF NOT EXISTS TEST_TABLE (key INT, value STRING)")
//          df4.explain(true)
//          df4.show()
//          sparkSession.sql("INSERT INTO TEST_TABLE VALUES (1,'2')")
//          val df5=sparkSession.sql("SELECT * FROM TEST_TABLE ")
//          df5.explain(true)
//          df5.show()
//    sparkSession.sql("DROP TABLE  IF EXISTS NEW_TABLE")
//      val df6=sparkSession.sql("CREATE TABLE NEW_TABLE AS SELECT * FROM OLD_TABLE")
//    df6.explain(true)
////    println("--------------------------------------------------------------")
//      val df7=sparkSession.sql("SELECT * FROM NEW_TABLE")
//      df7.show()

  }

}

// scalastyle:on