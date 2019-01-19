package com.bigdata.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * <b><code>WindowsWordCount</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/1/15 23:02.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object WindowsWordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("W")
    val ssc=new StreamingContext(conf,Seconds(5))

    val wordFlow=ssc.socketTextStream("localhost",9999).flatMap(_.split(" "))
      .map((_,1))

    wordFlow.reduceByKeyAndWindow((i: Int, i0: Int) =>i+i0,Duration.apply(30000),Duration.apply
    (10000)).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
