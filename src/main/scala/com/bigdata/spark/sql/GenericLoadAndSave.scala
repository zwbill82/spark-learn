package com.bigdata.spark.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>GenericLoadAndSave</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/22 20:09.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object GenericLoadAndSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("df").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //
    //    val df=sqlContext.read.load("src/main/resources/users.parquet")
    //    df.select("name","favorite_color").write.save("src/main/resources/parquet")
    //

    val loadPath = "src/main/resources/users.parquet"
    val savePath = "src/main/resources/json/"
    val df = sqlContext.read.format("parquet").load(loadPath)
    //df.write.format("json").save(savePath)
    //df.write.mode(SaveMode.Overwrite).format("json").save(savePath)
  }

}
