package com.bigdata.spark.sql.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>HiveDataSource</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/31 16:05.
  *
  * @todo 把虚拟机上的hive-site.xml,hdfs-site.xml,core-site.xml 拷贝到resource目录中
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object HiveDataSource {
  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setAppName("hive").setMaster("local")
    //val sc = new SparkContext(conf)
    //val sparkSession = new HiveContext(sc)


    val sparkSession = SparkSession.builder().
      appName("hive").
      master("local").
      enableHiveSupport()
      .getOrCreate()

    //    sparkSession.sql("DROP TABLE IF EXISTS student_infos")
    //    sparkSession.sql("create table student_infos (name STRING,age INT)")
    //    sparkSession.sql("DROP TABLE IF EXISTS student_scores")
    //    sparkSession.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)")
    //    sparkSession.sql("load data   inpath '" + args(0)+ "'  into table student_infos")
    //
    val goodStudent = sparkSession.sql("select a.*,b.score from student_infos a,student_scores b" +
      " where" +
      " a.name=b" +
      ".name and b" +
      ".score>80")

    goodStudent.show()
    sparkSession.sql("drop table if EXISTS student_good")
    goodStudent.write.saveAsTable("student_good")

    println("show student_good")
    sparkSession.table("student_good").show()
  }
}
