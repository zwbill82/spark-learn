package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>TransformTester</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/11 22:35.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object TransformTester {

  def testMap(rdd: RDD[Int]) = {
    rdd.map(_ * 2).foreach(println)
  }


  def main(args: Array[String]): Unit = {
    //创建
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    var counter = 0

    //构建集合
    //val rdd=sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10),1)
    val rdd = sc.parallelize(Range(1, 11))
    //元素乘以2
    rdd.map(_ * 2).foreach(x => {
      println(x);
      counter += 1
    })
    counter += 1;
    println("the value of counter is:" + counter)
    testMap(rdd)
    //偶数
    rdd.filter(_ % 2 == 0).foreach(println)


    //groupByKey
    val rdd2 = sc.parallelize(Array(("class1", 60), ("class1", 80), ("class2", 75), ("class2", 90)))
    rdd2.groupByKey().foreach(x => {
      println(x._1)
      //Iterable遍历
      x._2.foreach(println)
      println("++++++++++++++")
    })

    rdd2.groupByKey().map(x=>{(x._1,x._2.reduce(_ + _))}).foreach(println)

    //统计每个班级的分
    println ("===============starting reduceByKey========")
    rdd2.reduceByKey(_ + _).foreach(println)

    //排顺序
    rdd2.map(x=>{(x._2,x)}).sortByKey(false).map(x=>{x._2}).foreach(println)

    //join操作
    println("=======executing join test=======")
    val rddStudent=sc.parallelize(Array((1,"Tom"),(3,"Bob"),(2,"Jack"),(4,"Marry"),(10,"New1")))
    val rddScore=sc.parallelize(Array((1,100),(3,60),(2,70),(4,90),(3,20)))
    rddStudent.join(rddScore).map(x=>((x._1,x._2._1),x._2._2)).reduceByKey(_ + _).
      foreach(x=>printf("Student No:%d,Name:%s,Score:%d\n",x._1._1,x._1._2,x._2))

    rddStudent.join(rddScore).foreach(println)
    //

    val rddStudent1=sc.parallelize(Array((1,"Tom"),(3,"Bob"),(2,"Jack"),(4,"Marry"),(10,"New1"),
      (1,"sb")))
    val rddScore1=sc.parallelize(Array((1,100),(3,60),(2,70),(4,90),(3,20),(1,30),(2,80)))
    //val rdd3= rddStudent1.cogroup(rddScore1)
    val rdd3= rddScore1.cogroup(rddStudent1)
    rdd3.foreach(println)

  }

}
