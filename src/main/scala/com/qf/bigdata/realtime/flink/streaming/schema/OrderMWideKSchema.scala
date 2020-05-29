package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderMWideData, OrderWideData}
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 订单多维宽表数据
  * @param topic
  */
class OrderMWideKSchema(topic:String) extends KafkaSerializationSchema[OrderMWideData]{

  override def serialize(element: OrderMWideData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val orderID = element.orderID
    val productID = element.productID
    val ct = element.ct
    val tmp = orderID+ productID + element.pubID +ct
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)
    val value = JsonUtil.gObject2Json(element)

    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}
