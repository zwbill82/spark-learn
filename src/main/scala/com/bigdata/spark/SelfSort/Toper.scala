package com.bigdata.spark.SelfSort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>Toper</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/16 21:29.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object Toper {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("top").setMaster("local")
    val sc=new SparkContext(conf)
    sc.textFile("D:\\spark\\workplace\\data\\sortdata_1.txt").map(line=>{ (line.toInt,line) })
      .sortByKey(false).take(3).foreach(println)
  }
}
