package com.qf.bigdata.realtime.flink.streaming.assigner

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


/**
  * 用户行为数据事件时间辅助器
  * @param maxOutOfOrderness 最大延迟时间
  */
class UserLogsAssigner(maxOutOfOrderness :Long) extends AssignerWithPeriodicWatermarks[UserLogData]{

  //当前时间戳
  var currentMaxTimestamp :Long = java.lang.Long.MIN_VALUE


  /**
    * 水位生成
    * (1) 默认最小值
    * (2) 水位=当前时间-延迟时间
    * @return
    */
  override def getCurrentWatermark: Watermark ={
    var waterMark :Long = java.lang.Long.MIN_VALUE
    if(currentMaxTimestamp != java.lang.Long.MIN_VALUE){
      waterMark = currentMaxTimestamp - maxOutOfOrderness
    }
    new Watermark(waterMark)
  }


  /**
    * 事件时间提取
    * @param element 实时数据
    * @param previousElementTimestamp 之前数据的事件时间
    * @return
    */
  override def extractTimestamp(element: UserLogData, previousElementTimestamp: Long): Long = {
    //事件时间设置
    val eventTime = element.ct
    currentMaxTimestamp = Math.max(eventTime, currentMaxTimestamp)
    //返回
    eventTime
  }
}
