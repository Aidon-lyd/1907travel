package com.qf.bigdata.realtime.flink.streaming.etl.dw.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.{OrderDetailDataMapFun, OrderWideBCFunction}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderWideData}
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinfomation.QRealTimeDimTypeInfomation
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * 旅游产品订单实时数据处理：宽表输出
  * 宽表数据 = 旅游产品订单实时明细数据 + 旅游产品维表数据
  */
object OrdersWideDetail2ESHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersWideDetail2ESHandler")


  /**
    * 旅游产品订单实时宽表数据处理
    * @param appName 程序名称
    * @param fromTopic 数据源输入 kafka topic
    * @param groupID 消费组id
    * @param indexName 数据流输出ES索引
    */
  def handleOrdersWideJob(appName:String, groupID:String, fromTopic:String,  indexName:String):Unit = {
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
      val orderBoundedAssigner = new BoundedOutOfOrdernessTimestampExtractor[OrderDetailData](Time.milliseconds(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)) {
        override def extractTimestamp(element: OrderDetailData): Long = {
          element.ct
        }
      }

      /**
        * 4 旅游产品订单数据
        *   读取kafka中的原始明细数据并进行转换操作
        *   设置任务执行并行度setParallelism
        *   指定水位生成和事件时间(如果时间语义是事件时间的话)
        */
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
                                            .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
                                            .map(new OrderDetailDataMapFun())
                                            .assignTimestampsAndWatermarks(ordersPeriodicAssigner)


      /**
        * 5 mysql中提取旅游产品维度数据提取
        */
      val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInfomation.getProductDimFieldTypeInfos()
      val sql = QRealTimeConstant.SQL_PRODUCT
      val productDS: DataStream[ProductDimDO] = FlinkHelper.createProductDimDStream(env, sql, productDimFieldTypes)


      /**
        * 6 维表数据处理
        *   (1)维表数据状态描述对象
        *   (2)维表数据广播后(broadcast)形成广播数据流
        */
      val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])
      val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)


      /**
        * 7 旅游产品宽表数据(实时事实数据关联维表数据)
        *   实时事实数据：旅游产品订单数据
        *   维表数据：旅游产品维表
        *   关联处理函数：OrderWideBCFunction
        */
      val orderWideDStream :DataStream[OrderWideData] = orderDetailDStream
        .connect(dimProductBCStream)
        .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))
      //orderWideDStream.print("order.orderWideDStream---")

      /**
        * 8 宽表数据写入ES
        *   (1) 通过指定ES索引id用于后续流程进行数据搜索功能
        *   (2) ES接受map结构或json形式的数据(这里需要转换数据类型为map结构)
        */
      val esDStream:DataStream[String] = orderWideDStream.map(
        (value : OrderWideData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          //indexname,type,id=eid
          val eid = value.orderID
          result +=(QRealTimeConstant.KEY_ES_ID -> eid)
          val addJson = JsonUtil.object2json(result)
          addJson
        }
      )

      /**
        * 9 数据输出Sink
        *   (1) 自定义ESSink输出数据到ES索引
        *   (2) 通用类型ESSink：CommonESSink
        */
      val orderWideDetailESSink = new CommonESSink(indexName)
      esDStream.addSink(orderWideDetailESSink)
      esDStream.print()

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideDetail2ESHandler.err:" + ex.getMessage)
      }
    }
  }


  def main(args: Array[String]): Unit = {
   //应用程序名称
    val appName = "flink.OrdersWideDetail2ESHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //kafka消费组
    val groupID = "group.OrdersWideDetail2ESHandler"

    //宽表数据输出ES(明细搜索或交互式查询)
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_WIDE_DETAIL

    //旅游产品订单实时数据宽表加工处理并输出ES
    handleOrdersWideJob(appName, groupID, fromTopic,  indexName)
  }
}
