package com.bigdata.spark.sql.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>AutoPartitionJudge</code></b>
  * <p/>
  * parquet加载自动分区推断，集群执行
  * <p/>
  * <b>Creation Time:</b> 2018/12/23 21:02.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object AutoPartitionJudge {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("auto")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    val df=sqlContext.read.parquet(args(0))
    println ("schema data show")
    df.printSchema()
    println("data show")
    df.show()
  }
}
