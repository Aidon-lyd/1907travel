package com.qf.bigdata.realtime.flink.streaming.funs.cep

import java.util

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogPageViewAlertData, UserLogPageViewData}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
  * 用户行为日志涉及的复杂事件处理
  */
object UserLogsCepFun {

  /**
    * 页面浏览模式匹配处理函数
    */
  class UserLogsViewPatternProcessFun extends PatternProcessFunction[UserLogPageViewData,UserLogPageViewAlertData]{
    //处理匹配的数据
    override def processMatch(`match`: util.Map[String, util.List[UserLogPageViewData]],
                              ctx: PatternProcessFunction.Context,
                              out: Collector[UserLogPageViewAlertData]): Unit = {
      //取数据
      val datas :util.List[UserLogPageViewData] = `match`.getOrDefault(QRealTimeConstant.FLINK_CEP_VIEW_BEGIN, new util.ArrayList[UserLogPageViewData]())
      //判空
      if(!datas.isEmpty){
         for(data <- datas.iterator()){
           val viewAlertData = UserLogPageViewAlertData(data.userDevice, data.userId, data.userRegion,
             data.userRegionIP, data.duration, data.ct)

           //输出
           out.collect(viewAlertData)
         }
      }
    }
  }
}
