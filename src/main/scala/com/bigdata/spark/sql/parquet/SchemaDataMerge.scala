package com.bigdata.spark.sql.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>SchemaDataMerge</code></b>
  * <p/>
  * 元数据合并
  * <p/>
  * <b>Creation Time:</b> 2018/12/23 22:29.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object SchemaDataMerge {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SchemaDataMerge").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val squareDF = sc.parallelize(Range(1, 6)).map(x => {
      (x, x * x)
    }).toDF("value", "square")

    val cubeDF = sc.parallelize(Range(1, 6)).map(x => {
      (x, x * x * x)
    }).toDF("value", "cube")

    squareDF.write.mode("overwrite").parquet("src/ain/resources/parquet/merge")
    cubeDF.write.mode("Append").parquet("src/ain/resources/parquet/merge")

    val mergeDF=sqlContext.read.option("mergeSchema","true").parquet("src/ain/resources/parquet/merge")
    mergeDF.printSchema()
    mergeDF.show()

//    +-----+------+----+
//    |value|square|cube|
//    +-----+------+----+
//    |    1|     1|null|
//      |    2|     4|null|
//      |    3|     9|null|
//      |    4|    16|null|
//      |    5|    25|null|
//      |    1|  null|   1|
//      |    2|  null|   8|
//      |    3|  null|  27|
//      |    4|  null|  64|
//      |    5|  null| 125|
//      +-----+------+----+

  }
}
