package com.bigdata.spark.sql.datasource

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <b><code>All</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/24 22:38.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("JSONDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建学生成绩DataFrame
    val studentScoreDF = sqlContext.read.json("src/main/resources/data/students.json")
    studentScoreDF.registerTempTable("student_scores")

    // 查询出分数大于80分的学生成绩信息，以及学生姓名
    val goodDF = sqlContext.sql("select name,score  from student_scores where score>80")
    val goodStudentName = goodDF.rdd.map(row => {
      row(0).toString
    }).collect()

    // 创建学生基本信息DataFrame
    val rddStudentInfo = sc.parallelize(
      Array("{\"name\":\"Leo\", \"age\":18}", "{\"name\":\"Marry\", \"age\":17}",
        "{\"name\":\"Jack\", \"age\":19}"))

    val studentInfoDF = sqlContext.read.json(rddStudentInfo)
    // 查询分数大于80分的学生的基本信息
    studentInfoDF.registerTempTable("student_info")
    var querySql = "select name,age from student_info where name in ("
    for (x <- goodStudentName) {
      querySql += "'"
      querySql += x
      querySql += "',"
    }

    querySql = querySql.substring(0, querySql.length - 1) + ")"
    val goodStudentInfoDF = sqlContext.sql(querySql)
    goodStudentInfoDF.show()
    val goodStudentInfoRdd = goodStudentInfoDF.rdd.map(row => {
      (row.getAs[String]("name"), row
        .getAs[Long]
        ("age"))
    })


    // 将分数大于80分的学生的成绩信息与基本信息进行join
    val goodStudentsRDD =
      goodDF.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("score")) }
        .join(goodStudentInfoRdd)

    // 将rdd转换为dataframe
    val structType=new StructType( Array(StructField("name",StringType,true),StructField("score",
      IntegerType,true),StructField("age",IntegerType,true)))

    val finalDF= sqlContext.createDataFrame(goodStudentsRDD.map(x=>{Row(x._1,x._2._1.toInt,x
      ._2._2.toInt)}), structType)
   finalDF.show()
    // 将dataframe中的数据保存到json中
    finalDF.write.mode("overwrite").json("src/main/resources/json1")

  }
}
