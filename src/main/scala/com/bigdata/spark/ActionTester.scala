package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>ActionTester</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/15 16:06.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object ActionTester {
  def testReduce() = {
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val testRDD = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 9))
    println("testing reduce function")
    println(testRDD.reduce(_ + _))
  }

  def testCollect() = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Range(1, 21), 4)
    println("rdd foreach is running")
    rdd.foreach(println)
    println("collect test is running ")
    val result = rdd.collect()
    //复习基础语法，数组遍历
    for (x <- result) {
      println(x)
    }
  }

  def testCount() = {
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Range(1, 20), 1)
    println("testing count")
    println(rdd.count())
  }

  def testTake() = {
    val conf = new SparkConf().setAppName("take").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Range(1, 20), 1)
    println("testing take")
    rdd.take(3).foreach(println)
  }

  def testSaveAsTextFile() = {
    val conf = new SparkConf().setAppName("save").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Range(1, 20))
    rdd.saveAsTextFile("D:\\spark\\workplace\\data\\savetest.txt")
  }

  def main(args: Array[String]): Unit = {
    //testReduce()
    //testCollect()
    //testCount()
    //testTake()
    testSaveAsTextFile()
  }
}
