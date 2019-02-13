package com.bigdata.spark.sql.datasource

import org.apache.calcite.adapter.enumerable.RexImpTable.UserDefinedAggReflectiveImplementor
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>JDBCDataSource</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/25 22:56.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jdbc").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val connectMap=scala.collection.mutable.Map("url"->"jdbc:mysql://127.0.0.1:3306/test",
      "user"->"root",
      "password"->"root","dbtable"->"users")
    val userDF = sqlContext.read.format("jdbc").options(connectMap)
      .load()

    //以下为通过sql实现
    userDF.registerTempTable("users")


    connectMap("dbtable")="user_info"
    val userInfoDF = sqlContext.read.format("jdbc").options(connectMap).load()
    //userInfoDF.registerTempTable("user_info")

    userInfoDF.createOrReplaceTempView("user_info")

    val resultDF = sqlContext.sql("select a.*,b.addr from users a,user_info b where a.id=b.user_id")
    resultDF.printSchema()
    resultDF.show()

    println("DataFrame 写入数据 到MYSQL")
    connectMap("dbtable")="user_result"
    resultDF.write.mode("append").format("jdbc").options(connectMap).save()
    println("using RDD to implement")
    userDF.printSchema()
    val userRDD=userDF.rdd.map(row=>{(row.getInt(0),row.getString(1) )})
    val userInfoRDD=userInfoDF.rdd.map(row=>{(row.getInt(1),row.getString(2))})

    userRDD.join(userInfoRDD).foreach(println)


  }

}
