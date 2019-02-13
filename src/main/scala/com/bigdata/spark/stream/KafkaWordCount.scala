package com.bigdata.spark.stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * <b><code>KafkaWordCount</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/1/6 22:37.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Kafka1").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(5))

    val kafkaParams=Map("bootstrap.servers" -> "192.168.44.130:9092,192.168.44.131:9092,192.168.44.132:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topic=Array("TestTopic")
    val lines: InputDStream[ConsumerRecord[String, String]] =KafkaUtils.createDirectStream(ssc,
      PreferConsistent,Subscribe[String,String](topic,kafkaParams))

    lines.flatMap(line=>{line.value().split(" ")}).map((_,1)).reduceByKey(_ +_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
