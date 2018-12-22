package com.bigdata.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>DataFrameCreateTest</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/18 22:21.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object DataFrameCreateTest {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("df").setMaster("local")
    val sc=new SparkContext(conf)
   //sc.setLogLevel("ERROR")
    val sqlContext=new SQLContext(sc)
    val df=sqlContext.read.json("D:\\spark\\workplace\\data\\students.json")
    df.show()
    df.printSchema()

    df.select("id").show()
    df.select("id","name").show()
    //
    println(df.col("id"))

    //列值过滤
    println("filter test1")
    df.filter(df.col("id")>1  && df.col("age")===30 ).show()
    println ("filter test2")
    df.filter("id>1 and age=30").show()

    //某一列跟组
    df.groupBy("age").count().show()

  }

}
