package com.qf.bigdata.realtime.flink.streaming.etl.dw.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderDetailData
import com.qf.bigdata.realtime.flink.streaming.schema.OrderDetailKSchema
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游产品订单实时数据处理
  */
object OrdersDetailHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersDetailHandler")


  /**
    * 旅游产品订单数据实时ETL
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param toTopic 数据流输出 kafka topic
    */
  def handleOrdersJob(appName:String, groupID:String, fromTopic:String, toTopic:String):Unit = {
    try{
      /**
        * 1 Flink环境初始化
        *   流式处理的时间特征依赖(使用事件时间)
        */
      //注意：检查点时间间隔单位：毫秒
      val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
      val tc = TimeCharacteristic.EventTime
      val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, tc, watermarkInterval)


      /**
        * 2 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)

      /**
        * 3 设置事件时间提取器及水位计算
        *   方式：
        *     (1) 固定范围的水位指定(注意时间单位) BoundedOutOfOrdernessTimestampExtractor
        *     (2) 自定义实现AssignerWithPeriodicWatermarks 如 OrdersPeriodicAssigner
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)


      /**
        * 4 旅游产品订单数据
        *   (1) 设置水位及事件时间提取(如果时间语义为事件时间的话)
        *   (2)原始明细数据转换操作(json->业务对象OrderDetailData)
        */
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
                                           .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                           .map(new OrderDetailDataMapFun())
                                           .assignTimestampsAndWatermarks(ordersPeriodicAssigner)

      /**
        * 5 数据流(如DataStream[OrderDetailData])输出sink(如kafka、es等)
        *   (1) kafka数据序列化处理 如OrderDetailKSchema
        *   (2) kafka生产者语义：AT_LEAST_ONCE 至少一次
         */
      val kafkaSerSchema = new OrderDetailKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      //加入kafka摄入数据时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      orderDetailDStream.addSink(travelKafkaProducer)
      orderDetailDStream.print()

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersDetailHandler.err:" + ex.getMessage)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "flink.OrdersDetailHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"


    //kafka数据输出topic
    //val toTopic = QRealTimeConstant.TOPIC_ORDER_DW
    val toTopic = "travel_dw_orderdetail"

    //kafka消费组
    val groupID = "group.OrdersDetailHandler"

    //实时数据ETL
    handleOrdersJob(appName, groupID, fromTopic, toTopic)
  }
}
