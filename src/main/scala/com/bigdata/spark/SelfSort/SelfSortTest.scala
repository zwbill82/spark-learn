package com.bigdata.spark.SelfSort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>SelfSortTest</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/16 20:59.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object SelfSortTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sort").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\spark\\workplace\\data\\sortdata.txt")
    val rddmap = rdd.map(line => {
      (new SecondSortedKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt ), line)
    })

    val rddsorted=rddmap.sortByKey()
    rddsorted.foreach(x=>{println (x._2)})
  }
}
