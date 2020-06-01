package com.qf.bigdata.realtime.flink.streaming.funs.logs

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogClickData, UserLogData, UserLogPageViewData}
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * userLogData转换为UserLogPageViewData
 */
class UserLogViewDataMapFunc extends MapFunction[UserLogData,UserLogPageViewData]{
  //映射
  override def map(value: UserLogData): UserLogPageViewData = {
    //获取用户行为的扩展信息
    var targetID:String = ""

    val exts: String = value.exts
    if(StringUtils.isNotEmpty(exts)){
      //将数据转换成map
      val extmap:mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts).asScala
      targetID = extmap.getOrElse(QRealTimeConstant.KEY_TARGET_ID, "").toString
    }
    //封装成UserLogClickData
    UserLogPageViewData(value.sid,value.userDevice,value.userDeviceType,value.os,
      value.userId,value.userRegion,value.userRegionIP,value.longitude,value.latitude,value.manufacturer,
      value.carrier,value.networkType,value.duration,value.action,value.eventType,
      value.ct,targetID)
  }
}
