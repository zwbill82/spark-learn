package com.bigdata.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * <b><code>HDFSWordCount</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/1/6 21:01.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object HDFSWordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount")
    val ssc=new StreamingContext(conf,Seconds(30))

    //val file=ssc.textFileStream("/data/input")
    val file=ssc.textFileStream("D:\\spark\\workplace\\data")

    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _ ).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
