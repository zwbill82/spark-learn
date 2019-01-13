package com.bigdata.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * <b><code>UpdateStateByKeyTest</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/1/13 21:06.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object UpdateStateByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UpdateByKey").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(8))

    //checkpoint
    ssc.checkpoint("src/main/resources/data/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))

    // 这里两个参数
    // 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
    // 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
    // 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
    // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
    val wordCount = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (value <- values) {
        newValue += value
      }
      Option(newValue)
    })

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }


}
