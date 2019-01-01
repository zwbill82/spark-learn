package com.bigdata.spark.sql.fuction


import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}


/**
  * <b><code>UDAFFuction</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/1/1 13:04.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object UDAFFuction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("UDAF")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("warn")

    val rdd = sparkSession.sparkContext.parallelize(Array("Leo", "Marry", "Jack", "Tom", "Tom", "Tom", "Leo"))
      .map((name => {
        Row(name)
      }))
    val structType = StructType(Array(StructField("name", StringType, true)))

    val nameDF = sparkSession.sqlContext.createDataFrame(rdd, structType)
    nameDF.registerTempTable("names")

    sparkSession.udf.register("str_count", new StringCount)

    sparkSession.sql("select name,str_count(name) from names group by name").show()


    val scoreRdd = sparkSession.sparkContext.parallelize(Array(("Tom", 80), ("Jack", 90), ("Marry", 100),
      ("Bob", 70))).map(s => {
      Row(s._1, s._2)
    })

    val scoreStructType = StructType(Array(StructField("name", StringType, true),
      StructField("score", IntegerType, true)))

    val scoreDF = sparkSession.createDataFrame(scoreRdd, scoreStructType)

    scoreDF.registerTempTable("student_scores")

    sparkSession.udf.register("score_avg",new AvgCaculator)

    sparkSession.sql("select score_avg(score),avg(score)  from student_scores").show()


  }


}
