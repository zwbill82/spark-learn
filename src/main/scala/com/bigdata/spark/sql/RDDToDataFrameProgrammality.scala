package com.bigdata.spark.sql

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
  val sqlContext = new SQLContext(sc)

  //创建ROW-RDD
  val lines = sc.textFile("D:\\spark\\workplace\\data\\student.txt").map(line => {
    Row(line.split(",")(0).trim.toInt, line.split(",")(1).trim, line.split(",")(2).trim.toInt)
  })

  //动态方式构建元数据
  val structTypes = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))

  //进行RDD转换到DF
  val df = sqlContext.createDataFrame(lines, structTypes)

  df.registerTempTable("students")

  val resultdf = sqlContext.sql("select * from students where age>40")
  resultdf.show()

  Thread.sleep(100000)
}
