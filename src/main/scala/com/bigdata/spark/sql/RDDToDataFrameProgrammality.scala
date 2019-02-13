package com.bigdata.spark.sql


import better.files._
import better.files.File._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>RDDToDataFrameProgrammality</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/21 22:59.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object RDDToDataFrameProgrammality extends App {
  val conf = new SparkConf().setAppName("df").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("error")
  val sqlContext = new SQLContext(sc)

  //创建ROW-RDD
  val lines = sc.textFile("src/main/resources/data/student.txt").map(line => {
    Row(line.split(",")(0).trim.toInt, line.split(",")(1).trim, line.split(",")(2).trim.toInt)
  })

  //成绩RDD
  val score = sc.textFile("src/main/resources/data/score.txt").map(line => {
    Row(line.split(",")(0).trim.toInt, line.split(",")(1).trim.toInt)
  })

  //动态方式构建元数据
  val structTypes = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))

  val structScore = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("score", IntegerType, true)
  ))
  //进行RDD转换到DF
  val df = sqlContext.createDataFrame(lines, structTypes)
  val dfScore = sqlContext.createDataFrame(score, structScore)

  df.registerTempTable("students")
  dfScore.registerTempTable("score")

  val resultdf = sqlContext.sql("select a.*,b.score from students as  a,score  as b  where a.id=b" +
    ".id and " +
    "age>30")
  resultdf.show()
  // resultdf.repartition(1).write.format("com.databricks.spark.csv").save
  //("src/main/resources/result")
  //val file:java.nio.file.Path="src/main/resources/result"
  val basePath = java.nio.file.Paths.get(".").toAbsolutePath().normalize()
  val targetPath1 = java.nio.file.Paths.get(basePath.toString(), "src/main/resources/result/a.txt")
  val targetPath2 = java.nio.file.Paths.get(basePath.toString(), "src/main/resources/result_tmp")
  resultdf.coalesce(1).write
    .format("com.databricks.spark.csv")
    .mode("overwrite")
    .save(targetPath2.toString)

  targetPath2.toString.toFile.glob("part-00000-*.csv").foreach(f=>{
    f.copyTo(targetPath1.toString.toFile)
  })







  //Thread.sleep(100000)
}
