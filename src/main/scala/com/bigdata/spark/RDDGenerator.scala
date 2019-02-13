package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>RDDGenerator</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/9 21:54.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object RDDGenerator {
  def sumRDD(sc:SparkContext,seq:Array[Int]): Int ={
    sc.parallelize(seq,1).reduce(_+_)
  }

  def localFile(sc:SparkContext,filePath:String):Int={
      sc.textFile(filePath,2).flatMap(_.split(" ")).map(_.length()).reduce(_ + _)

  }


  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("sumRDD")
    if ( args(0)=="local" ) conf.setMaster("local")
    val sc=new SparkContext(conf)
    printf("value is %d,\n" ,sumRDD(sc, Array(1,2,3,4,5,6,7,8,9,10) ))
    printf("file length is %d \n",localFile(sc, args(1)))


  }
}
