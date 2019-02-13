package com.bigdata.spark.SelfSort

/**
  * <b><code>SecondeSortedKey</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/12/16 20:52.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
class SecondSortedKey(val first: Int, val second: Int) extends Ordered[SecondSortedKey] with Serializable {
  override def compare(that: SecondSortedKey): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    }
    else {
      this.second - that.second
    }
  }
}
