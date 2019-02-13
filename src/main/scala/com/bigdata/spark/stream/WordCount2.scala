package com.bigdata.spark.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * <b><code>WordCount</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/1/5 22:31.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8080)
    val word: DStream[String] = line.flatMap(x => {
      x.split(" ")
    })

    word.map(w => {
      (w, 1)
    }).reduceByKey(_ + _).print()



    ssc.start()
    //ssc.awaitTermination()
    Thread.sleep(5000)

    ssc.stop(false)

    println("another ssc2")
    val ssc2=new StreamingContext(sc,Seconds(2))
    ssc2.socketTextStream("localhost",8081).print()
    ssc2.start()
    ssc2.awaitTermination()

  }
}
