package com.mfw.utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @program: sparkCode
  * @description: ${description}
  * @author: Liusengen
  * @create: 2019-02-25 16:21
  **/
object DateUtil {
  def getNowDate:String={
    var now:Date=new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var nowstr=dateFormat.format(now)
    nowstr
  }
  def main(args: Array[String]): Unit = {
      println(DateUtil.getNowDate)
  }

}
