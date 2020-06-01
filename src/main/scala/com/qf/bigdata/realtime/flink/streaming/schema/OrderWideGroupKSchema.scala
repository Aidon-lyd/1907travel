package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderWideTimeAggDimMeaData
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord


/**
  * 订单宽表聚合数据序列化
  */
class OrderWideGroupKSchema(topic:String) extends KafkaSerializationSchema[OrderWideTimeAggDimMeaData]{

  override def serialize(element: OrderWideTimeAggDimMeaData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val productType = element.productType
    val toursimType = element.toursimType
    val tmp = productType+ toursimType
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)
    val value = JsonUtil.gObject2Json(element)

    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}
