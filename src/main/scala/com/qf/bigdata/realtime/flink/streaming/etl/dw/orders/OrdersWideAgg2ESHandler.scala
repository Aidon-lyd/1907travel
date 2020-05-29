package com.qf.bigdata.realtime.flink.streaming.etl.dw.orders

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.{OrderDetailDataMapFun, OrderWideBCFunction}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderWideData}
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinfomation.QRealTimeDimTypeInfomation
import com.qf.bigdata.realtime.flink.streaming.sink.orders.OrdersWideAggESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
  * 旅游订单业务实时计算：基于ES局部更新实现累加功能
  */
object OrdersWideAgg2ESHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersWideAgg2ESHandler")


  /**
    * 旅游产品订单数据实时处理
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param indexName 数据流输出
    */
  def handleOrdersWideAccJob(appName:String, groupID:String,fromTopic:String,  indexName:String):Unit = {
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
        * 2 离线维度数据提取
        *   旅游产品维度数据
        */
      val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInfomation.getProductDimFieldTypeInfos()
      //mysql查询sql
      val sql = QRealTimeConstant.SQL_PRODUCT
      val productDS :DataStream[ProductDimDO] = FlinkHelper.createOffLineDataStream(env, sql, productDimFieldTypes).map(
        (row: Row) => {
          val productID = row.getField(0).toString
          val productLevel = row.getField(1).toString.toInt
          val productType = row.getField(2).toString
          val depCode = row.getField(3).toString
          val desCode = row.getField(4).toString
          val toursimType = row.getField(5).toString
          new ProductDimDO(productID, productLevel, productType, depCode, desCode, toursimType)
        }
      )

      /**
        *   3 维表数据提取
        *   (1)维表数据状态描述对象
        *   (2)维表数据广播后(broadcast)形成广播数据流
        */
      //产品维表状态描述对象
      val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])

      //产品维表广播数据流
      val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)


      /**
        * 4 kafka流式数据源
        *   kafka消费配置参数
        *   kafka消费策略
        */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)


      /**
        * 4 订单数据
        *   (1) 原始明细数据转换操作
        *   (2) 设置事件时间提取器及水位计算
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
                                           .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                           .map(new OrderDetailDataMapFun())
                                           .assignTimestampsAndWatermarks(ordersPeriodicAssigner)


      /**
        * 6 创建旅游产品宽表数据
        */
      val orderWideDStream :DataStream[OrderWideData] = orderDetailDStream.connect(dimProductBCStream)
        .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))


      /**
        * 7 数据输出Sink
        *   自定义ESSink输出
        *   (基于ES局部更新实现累加功能)
        */
      val windowOrderESSink = new OrdersWideAggESSink(indexName)
      orderWideDStream.addSink(windowOrderESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideAgg2ESHandler.err:" + ex.getMessage)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "flink.OrdersWideAgg2ESHandler"

    //kafka消费组
    val groupID = "group.OrdersWideAgg2ESHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //数据输出ES
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_WIDE_AGG

    //订单宽表聚合输出处理
    handleOrdersWideAccJob(appName, groupID, fromTopic, indexName)
  }
}
