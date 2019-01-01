package com.bigdata.spark.sql.fuction

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>RowNumber</code></b>
  * <p/>
  * Description开窗函数
  * <p/>
  * <b>Creation Time:</b> 2018/12/30 22:31.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object RowNumber {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("win").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    //每个分类取top3
    val top3DF = hiveContext.sql("select * from  ( select product,category,revenue ,row_number() " +
      "over " +
      "(partition" +
      " by category order by revenue desc) rank  from sales ) tmp where rank<=3")
      top3DF.write.mode("overwrite").saveAsTable("sale_top3")

    Thread.sleep(30000)
  }

}
