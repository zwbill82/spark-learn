package com.bigdata.spark.sql.fuction

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * <b><code>UDFFunction</code></b>
  * <p/>
  * Description 自定义函数
  * <p/>
  * <b>Creation Time:</b> 2019/1/1 11:21.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object UDFFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("UDF")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val rdd = sparkSession.sparkContext.makeRDD(Array("Jack", "Tomy", "Lily", "Bob"))

    val structType = new StructType(Array(StructField("name", StringType, true)))

    val nameDF = sparkSession.sqlContext.createDataFrame(rdd.map(x => {
      Row(x)
    }), structType)

    nameDF.registerTempTable("names")

    sparkSession.sql("select * from names").show()

    sparkSession.sqlContext.udf.register("strLen", (str: String) => str.length)

    sparkSession.sql("select name,strLen(name) from names").show()
  }
}
