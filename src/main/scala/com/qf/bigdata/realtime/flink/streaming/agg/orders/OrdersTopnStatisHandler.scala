package com.qf.bigdata.realtime.flink.streaming.agg.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderTrafficDimData, OrderTrafficDimMeaData}
import com.qf.bigdata.realtime.flink.streaming.schema.OrderStatisKSchema
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}


/**
  * 旅游订单业务数据的实时统计排名
  */
object OrdersTopnStatisHandler {

  //日志记录
  val logger: Logger = LoggerFactory.getLogger("OrdersTopnStatisHandler")


  /**
   * 基于窗口内的订单数据进行统计后的实时排名
   */
  def handleOrdersWindowTopNJob(appName: String, groupID: String, fromTopic: String, toTopic: String, topN: Long): Unit = {
    try {
      /**
       * 1 Flink环境初始化
       * 流式处理的时间特征依赖(使用事件时间)
       */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val watermarkInterval = QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val timeChar = TimeCharacteristic.EventTime
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, timeChar, watermarkInterval)

      /**
       * 2 读取kafka旅游产品订单数据并形成订单实时数据流
       */
      val orderDetailDStream: DataStream[OrderDetailData] = FlinkHelper.createOrderDetailDStream(env, groupID, fromTopic, timeChar)


      /**
       * 3 订单数据聚合操作
       * (1) 分组维度：产品ID(productID)、出游交通方式(traffic)
       * (2) 开窗方式：滚动窗口(基于事件时间) TumblingEventTimeWindows
       * (3) 允许延时时间：allowedLateness
       * (4) 聚合操作：aggregate
       */
      import org.apache.flink.api.scala._
      val aggDStream: DataStream[OrderTrafficDimMeaData] = orderDetailDStream
        .keyBy(
          (detail: OrderDetailData) => {
            OrderTrafficDimData(detail.productID, detail.traffic)
          }
        )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .aggregate(new OrderDetailTimeAggFun(), new OrderTrafficTimeWindowFun())


      /**
       * 4 topN排序
       * Sorted的数据结构TreeSet或者是优先级队列PriorityQueue
       * (1) TreeSet实现原理是红黑树,时间复杂度是logN
       * (2) 优先队列实现原理就是最大/最小堆,堆的构造复杂度是N, 读取复杂度是1
       * 目前场景对应的是写操作频繁那么选择红黑树结构相对较好
       */
      val topNDStream: DataStream[OrderTrafficDimMeaData] = aggDStream
        .keyBy(
          (detail: OrderTrafficDimMeaData) => {
            OrderTrafficDimData(detail.productID, detail.traffic)
          }
        )
        .window(TumblingEventTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE * 3)))
        .allowedLateness(Time.seconds(QRealTimeConstant.FLINK_ALLOWED_LATENESS))
        .process(new OrderTopNKeyedProcessFun(topN))
      topNDStream.print("order.topNDStream---:")


      /**
       * 5 数据流(如DataStream[OrderTrafficDimMeaData])输出sink(如kafka、es等)
       * (1) kafka数据序列化处理 如OrderStatisKSchema
       * (2) kafka生产者语义：AT_LEAST_ONCE 至少一次
       */
      val orderStatisKSchema = new OrderStatisKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        orderStatisKSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      //加入kafka摄入时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      topNDStream.addSink(travelKafkaProducer)

      env.execute(appName)
    } catch {
      case ex: Exception => {
        logger.error("OrdersTopnStatisHandler.err:" + ex.getMessage)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "qf.OrdersTopnStatisHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //统计结果输出
    //val toTopic = QRealTimeConstant.TOPIC_ORDER_DM_STATIS
    val toTopic = "travel_dm_orders_topn"

    //kafka消费组
    val groupID = "group.OrdersTopnStatisHandler"

    //订单统计排名
    val topN = 3l
    handleOrdersWindowTopNJob(appName, groupID, fromTopic, toTopic, topN)
  }
}
