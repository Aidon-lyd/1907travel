package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderDetailData
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 旅游产品订单kafka数据序列化
  * @param topic
  */
class OrderDetailKSchema(topic:String) extends KafkaSerializationSchema[OrderDetailData] with KafkaDeserializationSchema[OrderDetailData]  {

  /**
    * 反序列化
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): OrderDetailData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()

    val log :OrderDetailData = gson.fromJson(new String(value), classOf[OrderDetailData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: OrderDetailData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val productID = element.productID
    val traffic = element.traffic
    val tmp = productID + traffic
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: OrderDetailData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[OrderDetailData] = {
    return TypeInformation.of(classOf[OrderDetailData])
  }
}
