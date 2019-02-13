package com.bigdata.spark.sql


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}


/**
  * <b><code>RDDToDataFrame</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/19 22:57.
  *
  * @author zhangweibiao
  * @since hui-bigdat a-spark ${PROJECT_VERSION}
  */
object RDDToDataFrame extends App {

  //val sqlSession=new SparkSession()
  val conf = new SparkConf().setMaster("local").setAppName("df")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  case class student(id: Int, name: String, age: Int)

  //创建RDD-->student
  val rdd = sc.textFile("D:\\spark\\workplace\\data\\student.txt").map(_.split(",")).map(t => {
    new student(t(0).trim.toInt, t(1), t(2).trim.toInt)
  })

  //反射方式转换
  import sqlContext.implicits._

  val df = rdd.toDF("id1", "name1", "age1")
  df.registerTempTable("t_test")
  sqlContext.sql("select * from t_test where age1>40").rdd.foreach(println)


}
