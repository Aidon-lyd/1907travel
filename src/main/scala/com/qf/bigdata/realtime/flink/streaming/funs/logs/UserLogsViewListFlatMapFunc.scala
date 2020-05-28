package com.qf.bigdata.realtime.flink.streaming.funs.logs

import java.util

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogViewListData, UserLogViewListFactData}
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * 用户浏览产品列表数据扁平化处理
 * 需求：
 * pro1
 * * "exts": "{
 * * "travelSendTime":"202002",
 * * "travelTime":"9",
 * * "productLevel":"4",
 * * "targetIDS":"["P40","P46","P28","P62"]",
 * * "travelSend":"520381",
 * * "productType":"01"
 * * }"
 *
 */
class UserLogsViewListFlatMapFunc extends FlatMapFunction[UserLogViewListData,UserLogViewListFactData]{
  //对输入的数据进行处理
  override def flatMap(value: UserLogViewListData, out: Collector[UserLogViewListFactData]): Unit = {
    //获取用户行为的扩展信息
    val exts: String = value.exts
    var targetIDJson:String = ""
    if(StringUtils.isNotEmpty(exts)){
      //将数据转换成map
      val extmap:mutable.Map[String,AnyRef] = JsonUtil.gJson2Map(exts).asScala
      targetIDJson = extmap.getOrElse(QRealTimeConstant.KEY_TARGET_IDS, "").toString
      //拿去其它字段
      val travelSendTime: String = extmap.getOrElse(QRealTimeConstant.KEY_TRAVEL_SENDTIME, "").toString
      val travelTime: String = extmap.getOrElse(QRealTimeConstant.KEY_TRAVEL_TIME, "").toString
      val productLevel: String = extmap.getOrElse(QRealTimeConstant.KEY_PRODUCT_LEVEL, "").toString
      val travelSend: String = extmap.getOrElse(QRealTimeConstant.KEY_TRAVEL_SEND, "").toString
      val productType: String = extmap.getOrElse(QRealTimeConstant.KEY_PRODUCT_TYPE, "").toString

      //将targetIDJson转换成集合
      val targetIDs: util.List[String] = JsonUtil.gJson2List(targetIDJson)
      //循环
      for(targetID <- targetIDs.asScala){
        //封装UserLogViewListFactData
        val data: UserLogViewListFactData = new UserLogViewListFactData(value.sid, value.userDevice, value.userDeviceType, value.os,
          value.userID, value.userRegion, value.lonitude, value.latitude, value.manufacturer,
          value.carrier, value.networkType, value.duration, value.action, value.eventType, value.ct,
          targetID, value.hotTarget, travelSend, travelSendTime, travelTime, productLevel, productType)

        //输出
        out.collect(data)
      }
    }
  }
}
