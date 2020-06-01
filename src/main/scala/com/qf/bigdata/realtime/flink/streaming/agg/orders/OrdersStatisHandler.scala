package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.Date
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.OrderStatisWindowProcessFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderDetailSessionDimData, OrderDetailStatisData}
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.streaming.trigger.{OrdersStaticCountTrigger, OrdersStatisTimeTrigger}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游订单业务实时计算
  * 基于订单数量触发订单数据统计任务
 *
 * 需求：
 * 1、基于条件（订单数量）触发订单任务统计
 * 2、根据traffic、hourtime维度，，来统计orders、users、totalFee三个指标
 * 3、将结果打入es
 * 需求2：
 * 1、基于时间触发订单任务统计
 * 2、根据traffic、hourtime维度，，来统计orders、users、totalFee三个指标
 * 3、将结果打入es
 *
  */
object OrdersStatisHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersStatisHandler")


  /**
    * 实时数据综合统计
    * 基于订单数据量作为触发条件进行窗口分组聚合
    */
  def handleOrdersStatis4CountJob(appName:String, groupID:String, fromTopic:String, indexName:String, maxCount:Long):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用处理时间)
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
      orderDetailDStream.print("orderDetailDStream->")

      /**
        * 3 基于订单数据量触发的订单统计
        *  (1) 分组维度：小时粒度的时间维度(hourTime)+出游交通方式(traffic)
        *      下面注释的地方为另一种小时时间提取方法
        *  (2) 开窗方式：基于处理时间的滚动窗口
        *  (3) 窗口触发器：自定义基于订单数据量的窗口触发器OrdersStatisCountTrigger
        *  (4) 数据处理函数：OrderStatisWindowProcessFun
        */
      val statisDStream:DataStream[OrderDetailStatisData] = orderDetailDStream
        .keyBy((detail:OrderDetailData) => {
          val hourTime = CommonUtil.formatDate4Timestamp(detail.ct, QRealTimeConstant.FORMATTER_YYYYMMDDHH)
          OrderDetailSessionDimData(detail.traffic, hourTime)
          }
         )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_MAX_SIZE)))
        .trigger(new OrdersStaticCountTrigger(maxCount))
        .process(new OrderStatisWindowProcessFun())
      statisDStream.print("statisDStream->")

      /**
        * 4 聚合数据写入ES
        *   (1) ES接收json或map结构数据，插入的id值为自定义索引主键id(为下游使用方搜索准备，如果采用es自生成id方式则不用采用此方式)
        */
      val esDStream:DataStream[String] = statisDStream.map(
        (value : OrderDetailStatisData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          val eid = value.traffic+QRealTimeConstant.BOTTOM_LINE+value.etTime
          result.put(QRealTimeConstant.KEY_ES_ID, eid)
          val addJson = JsonUtil.object2json(result)
          addJson
        }
      )
      esDStream.print("order.count.esDStream---")

      /**
        * 5 订单统计结果数据输出Sink
        *   自定义ESSink输出
        */
      val orderWideDetailESSink = new CommonESSink(indexName)
      esDStream.addSink(orderWideDetailESSink)
      esDStream.print("sink->")

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersStatisHandler.err:" + ex.getMessage)
      }
    }
  }


  /**
    * 实时数据综合统计
    * 基于处理时间作为触发条件进行窗口分组聚合
    */
  def handleOrdersStatis4ProcceTimeJob(appName:String, groupID:String, fromTopic:String, indexName:String,maxInternal:Long):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用处理时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val timeChar = TimeCharacteristic.ProcessingTime
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, timeChar, watermarkInterval)

      /**
        * 2 读取kafka旅游产品订单数据并形成订单实时数据流
        */
      val orderDetailDStream:DataStream[OrderDetailData] = FlinkHelper.createOrderDetailDStream(env, groupID, fromTopic,timeChar)


      /**
        * 3 基于处理触发的订单统计
        *  (1) 分组维度：小时粒度的时间维度(hourTime)+出游交通方式(traffic)
        *  (2) 开窗方式：基于处理时间的滚动窗口
        *  (3) 窗口触发器：自定义基于订单数据量的窗口触发器OrdersStatisTimeTrigger
        *  (4) 数据处理函数：OrderStatisWindowProcessFun
        */
      val statisDStream:DataStream[OrderDetailStatisData] = orderDetailDStream.keyBy(
        (detail:OrderDetailData) => {
          val hourTime = CommonUtil.formatDate4Timestamp(detail.ct, QRealTimeConstant.FORMATTER_YYYYMMDDHH)
          OrderDetailSessionDimData(detail.traffic, hourTime)
        }
      )
        .window(TumblingProcessingTimeWindows.of(Time.days(QRealTimeConstant.COMMON_NUMBER_ONE),  Time.hours(-8))) //每天5分钟触发
        .trigger(new OrdersStatisTimeTrigger(maxInternal, TimeUnit.MINUTES))
        .process(new OrderStatisWindowProcessFun())
      statisDStream.print(s"order.statisDStream[${CommonUtil.formatDate4Def(new Date())}]---:")

      /**
        * 4 聚合数据写入ES
        *   (1) ES接收json或map结构数据，插入的id值为自定义索引主键id(为下游使用方搜索准备，如果采用es自生成id方式则不用采用此方式)
        */
      val esDStream:DataStream[String] = statisDStream.map(
        (value : OrderDetailStatisData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          val eid = value.traffic+QRealTimeConstant.BOTTOM_LINE+value.etTime
          result.put(QRealTimeConstant.KEY_ES_ID, eid)
          val addJson = JsonUtil.object2json(result)
          addJson
        }
      )
      esDStream.print("order.time.esDStream---")

      /**
        * 5 数据输出Sink
        *   自定义ESSink输出
        */
      val orderWideDetailESSink = new CommonESSink(indexName)
      esDStream.addSink(orderWideDetailESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersStatisHandler.err:" + ex.getMessage)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "qf.OrdersStatisHandler"

    //kafka消费组
    val groupID = "group.OrdersStatisHandler"

    //kafka数据消费topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //定量触发窗口计算
    val maxCount = 50
    val indexNameCount = "travel_orders_count_statis"
    handleOrdersStatis4CountJob(appName, groupID, fromTopic, indexNameCount, maxCount)

    //定时触发窗口计算
    val maxInternal = 1
    val indexNameTime = "travel_orders_time_statis"
    //handleOrdersStatis4ProcceTimeJob(appName, groupID, fromTopic, indexNameTime, maxInternal)
  }
}
