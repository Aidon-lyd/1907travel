package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.UserLogPageViewAlertData
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 页面浏览异常告警数据
  * @param topic
  */
class UserLogsViewAlertKSchema(topic:String) extends KafkaSerializationSchema[UserLogPageViewAlertData] with KafkaDeserializationSchema[UserLogPageViewAlertData] {

  /**
    * 反序列化
    * @return
    */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogPageViewAlertData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()
    val log :UserLogPageViewAlertData = gson.fromJson(new String(value), classOf[UserLogPageViewAlertData])
    log
  }

  /**
    * 序列化
    * @param element
    * @return
    */
  override def serialize(element: UserLogPageViewAlertData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val userID = element.userId
    val userRegion = element.userRegion
    val userRegionIP = element.userRegionIp
    val tmp = userID+ userRegion+ userRegionIP
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogPageViewAlertData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogPageViewAlertData] = {
    return TypeInformation.of(classOf[UserLogPageViewAlertData])
  }

}
