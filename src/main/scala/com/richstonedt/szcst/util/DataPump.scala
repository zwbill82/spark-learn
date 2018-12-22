package com.richstonedt.szcst.util

/**
  * <b><code>DataPump</code></b>
  * <p/>
  * Description
  * <p/>
  * <b>Creation Time:</b> 2018/11/16 17:40.
  *
  * @author zhangweibiao
  * @since hui-bigdata-spark ${PROJECT_VERSION}
  */
object DataPump {

  /**
    * 产生一个范围内的随机整数
    * @param startNum 开始号码
    * @param endNum  结束号码
    * @return 随机数
    */
  def randInt(startNum:Int,endNum:Int):Int= {
    var result=0
    do
      result= (new util.Random).nextInt(endNum +1 )
    while (result > endNum || startNum > result)
    result
  }

  def genUserCgi(userCgi:Array[UserCgi],msisdnList:Array[String]): Unit ={
    var i=0
    //区域ID
    var areaId=0
    val cgiList=new CgiList
    //
    var cgiIndex=0
    for(msisdn <- msisdnList ){
      var userCgiSingle=new UserCgi
      areaId=randInt(4,24)
      cgiIndex=randInt(0,19)
      userCgiSingle.msisdn=msisdn
      userCgiSingle.cgi=cgiList.cgiList(areaId)(cgiIndex)
      userCgi(i)=userCgiSingle
      i +=1
    }

  }

  def main(args: Array[String]): Unit = {
    //定义10000个种子号码，尾号为0到9999
    val msisdnTail = Array(0 to 9999: _*)
    //format 处理，生成11位移动号码
    val msisdnList = msisdnTail.map((tail: Int) => "1880755" + "%4d".format(tail))

    //固定分配 号码对应区域
    var userCgi=new Array[UserCgi](10000)

    genUserCgi(userCgi,msisdnList)
    for (x<-userCgi) {
      println(x.msisdn +":" + x.cgi )
    }


  }

}
