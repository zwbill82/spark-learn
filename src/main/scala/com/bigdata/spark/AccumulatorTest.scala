package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>AccumulatorTest</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/15 22:00.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object AccumulatorTest {
  def testVarAccu() = {
    var sum = 0;
    val conf = new SparkConf().setAppName("var").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Range(1, 20), 3)
    rdd.foreach(x => {
      //每个分区一个task,有各自的变量副本
      sum += x
      println(sum)
    })
    //driver中还是0
    println(sum)
  }

  def testAccu() = {
    val conf = new SparkConf().setAppName("acc").setMaster("local")
    val sc = new SparkContext(conf)
    //定义acc变量
    //val sum = sc.accumulator(0)
    val sum=sc.longAccumulator
    println(sum)
    val rdd = sc.parallelize(Range(1, 20), 3)
    rdd.foreach(num => sum.add(num))
    println(sum.value)
  }

  def main(args: Array[String]): Unit = {
    //testVarAccu()
    testAccu()
  }
}
