package com.bigdata.spark.sql.fuction

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * <b><code>AvgCaculator</code></b>
  * <p/>
  * Description 计算平均值
  * <p/>
  * <b>Creation Time:</b> 2019/1/1 13:21.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
class AvgCaculator extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(StructField("score",IntegerType,true)))

  override def bufferSchema: StructType ={
    StructType(Array(StructField("sum",IntegerType,true),
    StructField("counter",IntegerType,true ) ))}

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0
    buffer(1)=0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Int](0) + input.getAs[Int](0)
    buffer(1)=buffer.getAs[Int](1)+1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
    buffer1(1)=buffer1.getAs[Int](1) +buffer2.getAs[Int](1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)*1.0/buffer.getAs[Int](1)
  }
}
