package com.bigdata.spark.sql.zhonghe

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * <b><code>WordsUV</code></b>
  * <p/>
  * 每日top3热点搜索词统计案例
  * <p/>
  * <b>Creation Time:</b> 2019/1/2 22:46.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object WordsUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UV").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    //search condition 查询条件模拟
    val filterParameters = mutable.HashMap("city" -> Array("beijing"), "platform" -> Array(
      "android"),
      "version" -> Array
      ("1.0", "3.0", "2.0"))

    val broadcastFilterParameters = sparkSession.sparkContext.broadcast(filterParameters)

    //合乎条件的记录
    val filterRDD = sparkSession.sparkContext.textFile("src/main/resources/data/wordofsearch.txt")
      .filter(line => {
        val city = line.split(":")(3)
        val platform = line.split(":")(4)
        val version = line.split(":")(5)
        val rddFilterParameters = broadcastFilterParameters.value
        if (rddFilterParameters("city").length > 0 && !rddFilterParameters("city").contains(city)) {
          false
        }
        else if (rddFilterParameters("platform").length > 0 && !rddFilterParameters("platform").contains(platform)) {
          false
        }
        else if (rddFilterParameters("version").length > 0 && !rddFilterParameters("version").contains(version)) {
          false
        }
        else {
          true
        }
      })

    //转换成日期，热点字
    val sortUVRDD = filterRDD.map(line => {
      val record = line.split(":")
      ((record(0), record(2)), record(1))
    }).sortByKey()

    sortUVRDD.foreach(println)
    sortUVRDD.groupByKey()
      .map(x => {
        val users = x._2.toList.distinct //UV值
        (x._1._1, (users.size, x._1._2))
      }).groupByKey().flatMap(r => {
      val top3 = r._2.toList.sortBy(r => r._1).reverse.take(3)
      top3.map(x => {
        (r._1, (x._2, x._1))
      })
    }).foreach(println)


  }

}
