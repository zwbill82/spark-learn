package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>WordCount</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/1 23:12.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object WordCount {

  def countWords(sc: SparkContext, filePath: String): Unit = {
    sc.textFile(filePath).map(_.split
    (",")(0)).map((_, 1)).reduceByKey(_ + _).map(x=>{(x._2,x)}).sortByKey(false).map(x=>x._2)
      .foreach(println)
  }

  /**
    *
    * @param args  第一个模式,第二个：路径
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount")
    if ( args(0)=="local" ) conf.setMaster("local")
    val sc = new SparkContext(conf)
    //countWords(sc, WordCount.getClass.getClassLoader.getResource("demo.txt").toString)
    countWords(sc ,args(1))
  }

}
