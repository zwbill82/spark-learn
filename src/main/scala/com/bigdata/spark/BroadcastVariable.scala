package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>BroadcastVariable</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/15 21:44.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("broad").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.parallelize(Range(1,40),4)
    val factor=sc.broadcast(5)
    rdd.map(_* factor.value).foreach(println)
  }

}
