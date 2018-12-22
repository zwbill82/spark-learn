package com.bigdata.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>PersistTester</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/15 21:06.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object PersistTester {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("persist").setMaster("local")
    val sc = new SparkContext(conf)
    var beginTime = System.currentTimeMillis()
    val rdd = sc.textFile("D:\\spark\\workplace\\data\\test.txt")
    rdd.persist(StorageLevel.MEMORY_ONLY)
    var counter=rdd.count()
    var endTime = System.currentTimeMillis()
    printf("time eclipse is:%d, and file lines is  %d \n",endTime-beginTime ,counter)

    beginTime = System.currentTimeMillis()
    counter=rdd.count()
    endTime = System.currentTimeMillis()
    printf("another time eclipse is:%d, and file lines is  %d \n",endTime-beginTime ,counter)

  }
}
