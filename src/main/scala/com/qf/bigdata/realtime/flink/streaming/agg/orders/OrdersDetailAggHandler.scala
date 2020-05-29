package com.qf.bigdata.realtime.flink.streaming.agg.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.{OrderDetailTimeAggFun, OrderDetailTimeWindowFun}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailAggDimData, OrderDetailData, OrderDetailTimeAggDimMeaData}
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游订单业务实时计算
 * 1、需求和ordersAggCacheHandler一模一样，，只是将结果数据打入es中
  */
object OrdersDetailAggHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersDetailAggHandler")

  /**
    * 旅游产品订单数据实时聚合
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param indexName 数据流输出
    */
  def handleOrdersAggWindowJob(appName:String, groupID:String, fromTopic:String, indexName:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val timeChar = TimeCharacteristic.EventTime
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, timeChar, watermarkInterval)

      /**
        * 2 读取kafka旅游产品订单数据并形成订单实时数据流
        */
      val orderDetailDStream:DataStream[OrderDetailData] = FlinkHelper.createOrderDetailDStream(env, groupID, fromTopic,timeChar)

      /**
        * 3 开窗聚合操作
        * (1) 分组维度列：用户所在地区(userRegion),出游交通方式(traffic)
        * (2) 聚合结果数据(分组维度+度量值)：OrderDetailTimeAggDimMeaData
        * (3) 开窗方式：滚动窗口TumblingEventTimeWindows
        * (4) 允许数据延迟：allowedLateness
        * (5) 聚合计算方式：aggregate
        */
      val aggDStream:DataStream[OrderDetailTimeAggDimMeaData] = orderDetailDStream
        .keyBy(
        (detail:OrderDetailData) => OrderDetailAggDimData(detail.userRegion, detail.traffic)
      )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderDetailTimeWindowFun())



      /**
        * 4 聚合数据写入ES
        *   (1) ES接收json或map结构数据，插入的id值为自定义索引主键id(为下游使用方搜索准备，如果采用es自生成id方式则不用采用此方式)
        */
      val esDStream:DataStream[String] = aggDStream.map(
        (value : OrderDetailTimeAggDimMeaData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          val eid = value.userRegion+QRealTimeConstant.BOTTOM_LINE+value.traffic
          result +=(QRealTimeConstant.KEY_ES_ID -> eid)
          val json = JsonUtil.gObject2Json(result)
          json
        }
      )

      /**
        * 5 订单统计结果数据输出Sink
        *   自定义ESSink输出
        */
      val orderAggESSink = new CommonESSink(indexName)
      esDStream.addSink(orderAggESSink)
      esDStream.print()

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersDetailAggHandler.err:" + ex.getMessage)
      }
    }
  }



  def main(args: Array[String]): Unit = {
   //应用程序名称
    val appName = "qf.OrdersDetailAggHandler"

    //kafka消费组
    val groupID = "group.OrdersDetailAggHandler"

    //kafka数据消费topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //订单统计数据输出ES
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_STATIS

    //实时处理第二层：开窗统计
    handleOrdersAggWindowJob(appName, groupID, fromTopic, indexName)
  }
}
