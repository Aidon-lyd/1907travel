package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderTrafficDimMeaData
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 订单统计数据kafka序列化
  *
  * @param topic
  */
class OrderStatisKSchema(topic:String) extends KafkaSerializationSchema[OrderTrafficDimMeaData] with KafkaDeserializationSchema[OrderTrafficDimMeaData] {



  /**
    * 反序列化
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): OrderTrafficDimMeaData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()
    val log :OrderTrafficDimMeaData = gson.fromJson(new String(value), classOf[OrderTrafficDimMeaData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: OrderTrafficDimMeaData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val productID = element.productID
    val traffic = element.traffic
    val tmp = productID + traffic
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: OrderTrafficDimMeaData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[OrderTrafficDimMeaData] = {
    return TypeInformation.of(classOf[OrderTrafficDimMeaData])
  }

}
