package com.bigdata.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * <b><code>TransformTest</code></b>
  * <p/>
  * transform算子使用s
  * <p/>
  * <b>Creation Time:</b> 2019/1/14 22:47.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("T").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(8))

    //创建黑名单RDD
    val blackRdd=ssc.sparkContext.parallelize(Array(("tom",true)))

    //广告日志流
    //日志格式 date name 如 20190113 leo
    val logDstream=  ssc.socketTextStream("localhost",9999).map(line=>{
        ( line.split(" ")(1),line)
      })


    /*
    tranform作用：内部可转找成为RDD ，与普通RDD join操作

     */
    logDstream.transform(logRdd=>{
      logRdd.leftOuterJoin(blackRdd).filter(tuple=>{
        !tuple._2._2.getOrElse(false)
      }).map(tuple=>{tuple._2._1})
    }).print()

    ssc.start()

    ssc.awaitTermination()


  }

}
