package com.qf.bigdata.realtime.flink.streaming.agg.orders

import java.util.Date
import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersAggFun.OrderCustomerStatisKeyedProcessFun
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderWideBCFunction
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderWideAggDimData, OrderWideCustomerStatisData, OrderWideData}
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinfomation.QRealTimeDimTypeInfomation
import com.qf.bigdata.realtime.flink.streaming.sink.CommonESSink
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * 旅游产品订单业务
  * 订单数据综合统计
  */
object OrdersCustomerStatisHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("OrdersCustomerStatisHandler")


  /**
    * 实时数据综合统计
    * 基于数据量作为触发条件进行窗口分组聚合
    */

  def handleOrdersWideStatisCustomerJob(appName:String, groupID:String, fromTopic:String, indexName:String,maxCount :Long, maxInterval :Long):Unit = {
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
        * 2 创建广播数据流(mysql维表数据流)
        *   (1) 读取mysql数据(flink-jdbc)
        *   (2) 产品维表广播数据描述对象productMSDesc
        *   (3) 产品维表广播数据流
        */
      val productDimFieldTypes :List[TypeInformation[_]] = QRealTimeDimTypeInfomation.getProductDimFieldTypeInfos()
      //mysql查询sql
      val sql = QRealTimeConstant.SQL_PRODUCT
      //mysql维度数据流(为了使用方便进行数据类型转换)
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
      val productMSDesc = new MapStateDescriptor[String, ProductDimDO](QRealTimeConstant.BC_PRODUCT, createTypeInformation[String], createTypeInformation[ProductDimDO])
      val dimProductBCStream :BroadcastStream[ProductDimDO] = productDS.broadcast(productMSDesc)



      /**
        * 3 读取kafka旅游产品订单数据并形成订单实时数据流
        */
      val orderDetailDStream:DataStream[OrderDetailData] = FlinkHelper.createOrderDetailDStream(env, groupID, fromTopic,timeChar)
      orderDetailDStream.print("orderDetailDStream->")
      /**
        * 4 旅游产品宽表数据
        *   (1) 基于维表广播方式
        *   (2) 事实数据流 connect 维表广播数据流
        *   (3) 创建宽表数据函数 OrderWideBCFunction
        */
      val orderWideDStream :DataStream[OrderWideData] = orderDetailDStream.connect(dimProductBCStream)
        .process(new OrderWideBCFunction(QRealTimeConstant.BC_PRODUCT))

      /**
        * 5 基于宽表明细数据进行聚合统计
        *   (1) 分组维度：产品类型(productType)，产品种类(出境|非出境)(toursimType)
        *   (2) 数据处理函数：OrderCustomerStatisKeyedProcessFun
        *      (基于数据量和间隔时间触发进行数据划分统计)
        */
      val statisDStream:DataStream[OrderWideCustomerStatisData] = orderWideDStream.keyBy(
        (wide:OrderWideData) => {
          OrderWideAggDimData(wide.productType, wide.toursimType)
          }
        )
        .process(new OrderCustomerStatisKeyedProcessFun(maxCount, maxInterval, TimeUnit.MINUTES))
      statisDStream.print(s"order.customer.statisDStream[${CommonUtil.formatDate4Def(new Date())}]---:")

      /**
        * 6 订单统计结果输出ES
        */
      val esDStream:DataStream[String] = statisDStream.map(
        (value : OrderWideCustomerStatisData) => {
          val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
          val eid = value.productType+QRealTimeConstant.BOTTOM_LINE+value.toursimType
          result +=(QRealTimeConstant.KEY_ES_ID -> eid)
          JsonUtil.gObject2Json(result)
        }
      )
      val orderAggESSink = new CommonESSink(indexName)
      esDStream.addSink(orderAggESSink)

      env.execute(appName)
    }catch {
      case ex: Exception => {
        logger.error("OrdersCustomerStatisHandler.err:" + ex.getMessage)
      }
    }

  }


  def main(args: Array[String]): Unit = {
     //应用程序名称
    val appName = "qf.handleOrdersStatisCustomerJob"

    //kafka消费组
    val groupID = "group.handleOrdersStatisCustomerJob"

    //kafka数据消费topic
    //val fromTopic = QRealTimeConstant.TOPIC_ORDER_ODS
    val fromTopic = "travel_orders_ods"

    //订单聚合结果输出ES
    val indexName = QRealTimeConstant.ES_INDEX_NAME_ORDER_CUSTOMER_STATIS

    //定量触发窗口计算
    val maxCount :Long = 10
    val maxInterval :Long = 1
    handleOrdersWideStatisCustomerJob(appName, groupID, fromTopic, indexName, maxCount, maxInterval)
  }
}
