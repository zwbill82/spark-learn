package com.bigdata.spark.SelfSort

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

/**
  * <b><code>ClassTopScore</code></b>
  * <p/>
  * 班级TOP 3
  * <p/>
  * <b>Creation Time:</b> 2018/12/16 22:02.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object ClassTopScore {
  def mapLineToPair(line: String): (String, Int) = {
    val lineSplit = line.split(" ")
    if (lineSplit.length >= 2) {
      (lineSplit(0), lineSplit(1).toInt)
    }
    else {
      ("None", 0)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("score").setMaster("local")
    val sc = new SparkContext(conf)
    sc.textFile("D:\\spark\\workplace\\data\\sortdata_2.txt").map(mapLineToPair)
      .groupByKey().flatMap(x=>{
      val topItem=x._2.toList.sorted.reverse.take(3)
      topItem.map(t=>{(x._1,t)})
    }).foreach(println )

//    scoreRDD.foreach(cs => {
//      println("class top 3 is :" + cs._1)
//      var i = 0
//      val loop = new Breaks
//      loop.breakable(
//        for (x <- cs._2.toArray.sortWith(_ > _)) {
//          if (i < 3) {
//            println(x)
//          }
//          else {
//            loop.break();
//          }
//          i += 1;
//        }
//      )
//      println("================")
//    }
//    )
  }

}
