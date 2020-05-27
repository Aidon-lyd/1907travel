package com.qf.bigdata.realtime.flink.util

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date

//时间工具类
object DateUtil {
  val PATTERN_YYYYMMDD =  DateTimeFormatter.ofPattern("yyyyMMdd")

  //将time时间戳转换成指定pattern的时间字符串
  def dateFormat(time: Long, pattern: String): String = {
    if (time <= 0 || null == pattern) {
      return null
    }
    val datetime = LocalDateTime.ofInstant(new Date(time).toInstant, ZoneId.systemDefault())
    datetime.format(DateTimeFormatter.ofPattern(pattern))
  }

  //将date类型时间转换成时间字符串
  def dateFormat(date: Date): String = {
    if (null == date) {
      return null
    }
    val datetime = LocalDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())
    datetime.format(PATTERN_YYYYMMDD)
  }
}
