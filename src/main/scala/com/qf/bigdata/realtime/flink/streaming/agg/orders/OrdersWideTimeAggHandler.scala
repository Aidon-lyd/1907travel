package com.qf.bigdata.realtime.flink.streaming.agg.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.{OrderWideTimeAggFun, OrderWideTimeWindowFun}
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun._
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderWideAggDimData, OrderWideData, OrderWideTimeAggDimMeaData}
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinfomation.QRealTimeDimTypeInfomation
import com.qf.bigdata.realtime.flink.streaming.schema.OrderWideGroupKSchema
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}


/**
 * 旅游产品订单宽表数据实时统计计算
 */
object OrdersWideTimeAggHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersWideTimeAggHandler")

  /**
   * 旅游产品订单数据实时统计
   * @param appName 程序名称
   * @param fromTopic 数据源输入 kafka topic
   * @param groupID 消费组id
   * @param toTopic 数据流输出 kafka topic
   */
  def handleOrdersWideAggWindowJob(appName:String, groupID:String, fromTopic:String, toTopic:String):Unit = {
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
       * 2 mysql维度数据提取
       *   (1) 旅游产品维度数据
       *   (2) 维表数据状态描述对象
       *   (3) 维表数据广播后(broadcast)形成广播数据流
       */
      val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInfomation.getProductDimFieldTypeInfos()
      val sql = QRealTimeConstant.SQL_PRODUCT
      val productDS: DataStream[ProductDimDO] = FlinkHelper.createProductDimDStream(env, sql, productDimFieldTypes)

      val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])
      val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)

      /**
       * 3 kafka流式数据源
       *   kafka消费配置参数
       *   kafka消费策略
       */
      val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)


      /**
       * 5 旅游产品订单数据
       *   原始明细数据转换操作(json->业务对象OrderDetailData)
       */
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .map(new OrderDetailDataMapFun())

      /**
       * 6 旅游产品宽表数据
       *  (1) 旅游订单数据(事实数据)
       *  (2) 产品维表数据
       */
      val orderWideGroupDStream :DataStream[OrderWideData] = orderDetailDStream.connect(dimProductBCStream)
        .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))

      /**
       * 7 基于订单宽表数据的聚合统计
       *  (1) 分组维度：产品类型(productType)+出境类型(toursimType)
       *  (2) 开窗方式：基于时间的滚动窗口
       *  (3) 数据处理函数：aggregate
       */
      val aggDStream:DataStream[OrderWideTimeAggDimMeaData] = orderWideGroupDStream
        .keyBy({
          (wide:OrderWideData) => OrderWideAggDimData(wide.productType, wide.toursimType)
        })
        .window(TumblingProcessingTimeWindows.of(Time.seconds(QRealTimeConstant.FLINK_WINDOW_SIZE)))
        .aggregate(new OrderWideTimeAggFun(), new OrderWideTimeWindowFun())



      /**
       * 8 数据输出Kafka
       */
      val orderWideGroupKSerSchema = new OrderWideGroupKSchema(toTopic)
      val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val travelKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        orderWideGroupKSerSchema,
        kafkaProductConfig,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

      //加入kafka摄入时间
      travelKafkaProducer.setWriteTimestampToKafka(true)
      aggDStream.addSink(travelKafkaProducer)
      aggDStream.print("aggDStream->")

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersWideTimeAggHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "qf.OrdersWideTimeAggHandler"

    //kafka消费组
    val groupID = "group.OrdersWideTimeAggHandler"

    //kafka数据消费topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //val toTopic = QRealTimeConstant.TOPIC_ORDER_MID
    val toTopic = "travel_dm_orders"


    //实时处理第二层：宽表处理
    handleOrdersWideAggWindowJob(appName, groupID, fromTopic, toTopic)
  }
}
