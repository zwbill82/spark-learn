package com.bigdata.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>ParquetOperation</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/22 22:11.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object ParquetOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PO").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val df = sqlContext.read.format("parquet").load("src/main/resources/users.parquet")
    df.printSchema()
    df.registerTempTable("users")
    val nameDF = sqlContext.sql("select name from users")
    //普通的方法
    nameDF.select("name").show()

    //rdd操作
    nameDF.rdd.map(row=>{"name:=" + row(0).toString }).foreach(println)
  }

}
