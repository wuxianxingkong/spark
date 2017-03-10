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
package org.apache.spark.examples.sql

// $example on:schema_inferring$
// $example off:schema_inferring$
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SessionState
// import org.apache.spark.sql.execution.datasources.index.lucenerdd.LuceneRDD
import org.apache.spark.sql.execution.datasources.index.lucenerdd._
import scala.reflect.ClassTag
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$

object SparkSQLTest {

  // $example on:create_ds$
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  case class Person(name: String, age: Long)
  // $example off:create_ds$


  private def test1(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources1/")
    df.createOrReplaceTempView("test1")
    sparkSession.sql("select * from test1").show();
    val df1=sparkSession.sql("insert into test1 values(1,'cuiguangfan')")
    println(sparkSession.sparkContext.getLocalProperties.get("spark.job.description"));
    df1.explain(true)
    sparkSession.sql("select * from test1").show();
  }
  private def test2(sparkSession: SparkSession) : Unit={
    sparkSession.sql("drop table if exists wikipage")
    sparkSession.sql("create table wikipage (docid int,page string) row format delimited fields terminated by '|' stored as textfile")
    val df=sparkSession.sql("load data local inpath '/home/cuiguangfan/IdeaProjects/LikeExplorer/source1/tfidf_test.tsv' into table wikipage")
    df.explain(true)
  }
  private def test3(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources1/")
    df.createOrReplaceTempView("test1")
    sparkSession.sql("select * from test1").show();
    val df1=sparkSession.sql("create index index_test_1 on table test1 (name) using com.xingkong.index")
    df1.explain(true)
    sparkSession.sql("select * from index_test_1").show();
    sparkSession.sql("describe extended test1").show();
    sparkSession.sql("describe extended index_test_1").show();

  }
  private def test4(spark: SparkSession) : Unit={
    spark.sql("select * from index_test_1").show();
  }
  private def test5(sparkSession: SparkSession) : Unit={
    val array = Array("Hello", "world")
    val rdd = LuceneRDD(sparkSession.sparkContext, array, "index_dir_1")
    val count = rdd.count
    val result = rdd.termQuery("_1", "hello", 10)
    result.foreach(println)
  }
  private def test6(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources1/")
    // val df=sparkSession.createDataFrame(rdd)
    df.printSchema()
    val luceneRDD = LuceneRDD(df,"test_test_1")
//    val results = luceneRDD.termQuery("name", "justin")
    val results = luceneRDD.termQuery("age", "30")
    println("result count: " + results.count)
    results.foreach(println)
  }
//  private def test7(sparkSession: SparkSession) : Unit={
//    val elem = Array("fear", "death", "water", "fire", "house")
//      .zipWithIndex.map{ case (str, index) =>
//      FavoriteCaseClass(str, index, 10L, 12.3F, s"${str}@gmail.com")}
//    val rdd = sparkSession.sparkContext.parallelize(elem)
//    val df=sparkSession.createDataFrame(rdd)
//    df.printSchema()
//    val luceneRDD = LuceneRDD(df,"test_test_1")
//    println("print luceneRDD inner:")
//    luceneRDD.count()
//    luceneRDD.foreach(println)
//    val results = luceneRDD.termQuery("name", "water")
//    results.foreach(println)
//  }
  private def test8(sparkSession: SparkSession) : Unit={
    sparkSession.sql("drop table if exists index_test_1")
    val df = sparkSession.read.json("examples/src/main/resources1/")
    df.createOrReplaceTempView("test1")
    sparkSession.sql("select * from test1").show();
    val df1=sparkSession.sql("create index index_test_1 on table test1 (name) using org.apache.spark.sql.index")
    df1.explain(true)
    sparkSession.sql("show tables").show()
  }
  private def test9(sparkSession: SparkSession) : Unit={
    val tableName = "test_test_1"
    val results = LuceneRDD(sparkSession, tableName).query("name", "just~")
    println(results.count)
    results.take(5).foreach(println)
  }
  private def test10(sparkSession: SparkSession) : Unit={
    // test index with specified columns
    val df = sparkSession.read.json("examples/src/main/resources3/")
    df.printSchema()
    val luceneRDD = LuceneRDD(df,"test_test_1",Seq[String]("name"), true)
    val results = luceneRDD.query("name", "just~")
    println("Result nums: " + results.count())
    results.foreach(println)
    // lucene中不论用什么方法索引数字类型，都无法使用termquery搜索到
  }
  private def test11(sparkSession: SparkSession) : Unit={
    // test index with specified columns
    val df = sparkSession.read.json("examples/src/main/resources4/")
    df.printSchema()
    df.show()
    val luceneRDD = LuceneRDD(df,"test_index",Seq[String]("title","body"), true)
    val results = luceneRDD.query("nothisfield","body:database",3)
    results.foreach(println)
    // 如果索引的词是停用词，那么是搜不到的，比如"A"等停用词
    // 对于非停用词，如"B"，就能搜到

    //验证了正确性：即，只对Seq指定的column建立了索引，并且，所有field均已存储
  }
  private def test12(sparkSession: SparkSession) : Unit={
    val df1 = sparkSession.read.json("examples/src/main/resources1/")
    df1.createOrReplaceTempView("table1")
    val df3 = sparkSession.read.json("examples/src/main/resources3/")
    df1.createOrReplaceTempView("table2")
    val result = sparkSession.sql("select * from table1 join table2 on table1.name = table2.name")
    result.explain(true)
    result.show()
    //result.collect().foreach(println)
    // 准备参考下join的语法树结构
    // 结论：没有帮助
  }
  private def test13(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources3/")
    df.createOrReplaceTempView("test1")
    df.describe()
    sparkSession.sql("select * from test1").show()
    sparkSession.sql("drop table if exists index_test_13")
    val df1=sparkSession.sql("create index index_test_13 on table test1 (name) using org.apache.spark.sql.index")
    df1.explain(true)
    val df2 = sparkSession.sql("select * from index_test_13 where complexQuery('name','i just like it','3')")
    df2.explain(true)
    df2.show()
//    sparkSession.sql("describe extended test1").show()
//    sparkSession.sql("describe extended index_test_13").show()

  }
  private def test14(sparkSession: SparkSession) : Unit={

    val df = sparkSession.read.json("examples/src/main/resources1/")
    df.createOrReplaceTempView("test1")
    val df1 = sparkSession.sql("select age from test1 where age between 1 and 30")
    df1.explain(true)
    df1.show()
  }
  private def test15(sparkSession: SparkSession) : Unit={
    val df2 = sparkSession.sql("select * from index_test_13 where complexQuery('name','just~','3')")
    //df2.explain(true)
    df2.show()
    // Reading from existed index: Right
  }
  private def test16(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources3/")
    df.createOrReplaceTempView("test1")
    val df1 = sparkSession.sql("select * from test1 where name like 'jus%' order by name limit 2")
    df1.explain(true)
    df1.show()
  }
  private def test17(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources4/")
    df.createOrReplaceTempView("articles")
    df.printSchema()
    sparkSession.sql("desc articles").show()
    sparkSession.sql("select * from articles").show()
    sparkSession.sql("drop table if exists test")
    val testdf = sparkSession.sql("create  table test as select * from articles")
    testdf.explain(true)
    sparkSession.sql("select * from test").show()
    sparkSession.sql("drop table if exists articles_index")
    val df1=sparkSession.sql("create index articles_index on table test (title,body) using org.apache.spark.sql.index")
    df1.explain(true)
//    val df2 = sparkSession.sql("select * from articles_index where queryparser('nothisfield','body:(Security implications of running MySQL as root) AND title:(security)','3')")
//    sparkSession.sql("select score from articles_index where queryparser('nothisfield','body:database','3')").explain(true)
    val df2 = sparkSession.sql("select body,id from articles_index where queryparser('nothisfield','body:database','3')")
    df2.explain(true)
    df2.show()
//    val df = sparkSession.read.json("examples/src/main/resources4/")
//    df.printSchema()
//    val luceneRDD = LuceneRDD(df,"index_test_14",Seq[String]("title","body"))
//    val results = luceneRDD.query("ff", "body:(Security implications of running MySQL as root)")
//    println("Result nums: " + results.count())
//    results.foreach(println)

    // 所有测试均已通过
  }
  private def test18(sparkSession: SparkSession) : Unit={
    var rdd = sparkSession.sparkContext.makeRDD(Seq("A","B","R","D","F"),2)
    rdd.zipWithIndex().collect.foreach(println)
  }
  private def test19(sparkSession: SparkSession) : Unit={
    val df = sparkSession.read.json("examples/src/main/resources4/")
    df.createOrReplaceTempView("articles")
    val df1 =  sparkSession.sql("select * from articles a join articles b on a.id=b.id")
    df1.explain(true)
    df1.show()
  }
  private def test20(sparkSession: SparkSession) : Unit={
    sparkSession.sql("drop table if exists usent_corpus_all")
    sparkSession.sql("create table if not exists usent_corpus_all (document string comment 'document content') ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '---END.OF.DOCUMENT---'")
//    sparkSession.sql("load data local inpath '/home/cuiguangfan/xingkong/DataSets/WestburyLab.NonRedundant.UsenetCorpus.txt' " +
//      "into table usent_corpus_all")
    sparkSession.sql("select count(*) from usent_corpus_all")
  }
  private def test21(spark: SparkSession) : Unit={
    val prefix="usenet_corpus"
    // 第几次实验
    val test_index=25343
    // 重分区数量
    val repartitionNum=24
    // val path="/home/cuiguangfan/IdeaProjects/ScalaExplorer/"
//    val df = spark.read.format("jdbc").option("url", "jdbc:mysql://133.133.134.118/test?serverTimezone=UTC").option("driver", "com.mysql.jdbc.Driver").option("dbtable", prefix+"_"+test_index).option("user", "root").option("password", "123456").load()
//    val df1 = df.repartition(repartitionNum)
//    df1.write.parquet("/opt/testdata/"+prefix+"_"+test_index+"_"+repartitionNum+"_test")
    spark.sql("drop table if exists "+prefix+"_"+test_index+"_"+repartitionNum+"_test")
    spark.sql("create  table "+prefix+"_"+test_index+"_"+repartitionNum+"_test using parquet options (path '/home/cuiguangfan/下载/"+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"')")
    spark.sql("select * from "+prefix+"_"+test_index+"_"+repartitionNum+"_test").show()
    spark.sql("drop table if exists "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"_index")

    spark.sql("create index "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"_index"+" on table "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"(body) using org.apache.spark.sql.index strategy quickway")

    val df3 = spark.sql("select * from "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"_index"+" where queryparser('nothisfield','body:person','3')")
//    df3.explain(true)
    df3.show()
    spark.sql("drop table if exists "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"_index_noquickway")

    spark.sql("create index "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"_index_noquickway"+" on table "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"(body) using org.apache.spark.sql.index")

    val df4 = spark.sql("select * from "+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"_index_noquickway"+" where queryparser('nothisfield','body:person','3')")
    //    df3.explain(true)
    df4.show()
  }
  private def test22(spark: SparkSession) : Unit={

    val prefix="usenet_corpus"
    // 第几次实验
    val test_index=0
    // 重分区数量
    val repartitionNum=3
    // val path="/home/cuiguangfan/IdeaProjects/ScalaExplorer/"
    //    val df = spark.read.format("jdbc").option("url", "jdbc:mysql://133.133.134.118/test?serverTimezone=UTC").option("driver", "com.mysql.jdbc.Driver").option("dbtable", prefix+"_"+test_index).option("user", "root").option("password", "123456").load()
    //    val df1 = df.repartition(repartitionNum)
    //    df1.write.parquet("/opt/testdata/"+prefix+"_"+test_index+"_"+repartitionNum+"_test")
    spark.sql("drop table if exists "+prefix+"_"+test_index+"_"+repartitionNum+"_test")
    spark.sql("create  table "+prefix+"_"+test_index+"_"+repartitionNum+"_test using parquet options (path '/home/cuiguangfan/下载/"+prefix+"_"+test_index+"_"+repartitionNum+"_test"+"')")
    val df = spark.sql("select * from "+prefix+"_"+test_index+"_"+repartitionNum+"_test")
    df.write.insertInto("")
    val luceneRDD = LuceneRDD(df,"test_index",Seq[String]("body"),true)
    val results=luceneRDD.query("nothisfield","body:person",3)
    results.foreach(println)
  }
  private def test23(spark: SparkSession) : Unit={
    val df = spark.read.parquet("/home/cuiguangfan/下载/tt")
    println(df.rdd.partitions.length)
  }
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Spark SQL basic example")
      .enableHiveSupport()
      .getOrCreate()       //.config("spark.some.config.option", "some-value")

    // For implicit conversions like converting RDDs to DataFrames
    // $example off:init_session$
    println(spark.conf.getAll)
    test17(spark)
    //    test10(spark)
    spark.stop()
  }
}
// scalastyle:on