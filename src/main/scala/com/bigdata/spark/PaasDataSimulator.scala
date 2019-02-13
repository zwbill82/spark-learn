package com.bigdata.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import better.files._

import org.apache.spark.{SparkConf, SparkContext}


/**
  * <b><code>PaasDataSimulator</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2019/2/9 22:14.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object PaasDataSimulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PAAS").setMaster("local")
    val sc = new SparkContext(conf)

    val filepath = "src/main/resources/data/test.txt"
    val despath = "src/main/resources/data/result/"
    val result="src/main/resources/result/"
    val rdd = sc.textFile(filepath)
    val rdd1 = rdd.map(line => {
      val words = line.split(",")
      (words(0), line)
    })

    rdd1.persist()

    val sf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    for (i <- 0 to 95) {
      cal.setTime(sf.parse(sf.format(new Date())))
      cal.add(Calendar.MINUTE, i * 15)
      //--特别的前一天的数据
      //cal.add(Calendar.HOUR,-24)
      rdd1.filter(x => {
        x._1.equals(cal.getTimeInMillis.toString)
      }).map(_._2).saveAsTextFile(despath + cal.getTimeInMillis.toString)

      //复制重新
      val sf2=new java.text.SimpleDateFormat("yyyyMMddHHmmss")
      (despath + cal.getTimeInMillis.toString).toFile.glob("part-*").foreach(f=>{
       f.copyTo((result+sf2.format(cal.getTime )).toFile)
     })


    }
  }
}
