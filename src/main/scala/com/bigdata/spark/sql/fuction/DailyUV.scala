package com.bigdata.spark.sql.fuction

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * <b><code>DailyUV</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/29 22:25.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UV").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //模拟日志
    val logArray = Array(
      "2018-12-01,122",
      "2018-12-01,123",
      "2018-12-01,124",
      "2018-12-01,122",
      "2018-12-02,122",
      "2018-12-02,322",
      "2018-12-02,152",
      "2018-12-02,124"
    )

    val structType = StructType(Array(StructField("date", StringType, true),
      StructField("userid", IntegerType, true)))

    val rdd = sc.parallelize(logArray).map(row => {
      Row(row.split(",")(0).toString,
        row.split(",")(1).toInt)
    })

    val userDF = sqlContext.createDataFrame(rdd, structType)

    userDF.groupBy("date").agg('date.alias("date2"),countDistinct('userid ).alias("counter"))
        .select ("date","counter")
    .show()


    //userDF.groupBy("date").agg('date,max('userid)).select("date")

    userDF.registerTempTable("user_logs")

//    sqlContext.sql("select date,count (distinct userid) as counter  from user_logs group by date")
//      .show()


  }

}
