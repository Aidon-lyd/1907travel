package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogData, UserLogPageViewData}
import com.qf.bigdata.realtime.util.{CommonUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 用户page浏览行为日志数据的序列化和反序列化
 * 1、实现我们的kafka的序列化和反序列化
 */
class UserLogsPageViewSchema(topic:String) extends KafkaSerializationSchema[UserLogPageViewData]
      with KafkaDeserializationSchema[UserLogPageViewData]{
  //序列化
  override def serialize(element: UserLogPageViewData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //获取到sid
    val sid: String = element.sid
    val userDevice = element.userDevice
    val targetID = element.targetId
    val key = sid + userDevice + targetID

    //将key对应使用md5加密
    val keys: String = CommonUtil.getMD5AsHex(key.getBytes())
    val value: String = JsonUtil.gObject2Json(element)
    //封装成ProducerRecord
    new ProducerRecord[Array[Byte], Array[Byte]](topic,keys.getBytes,value.getBytes)
  }

  //是否结尾
  override def isEndOfStream(nextElement: UserLogPageViewData): Boolean = {
    return false
  }

  //反序列化
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogPageViewData = {
    //取出key-value
    val value: Array[Byte] = record.value()
    //将我们的value值封装成userLogData对象
    val gson: Gson = new Gson()
    val data: UserLogPageViewData = gson.fromJson(new String(value), classOf[UserLogPageViewData])
    data
  }

  //获取数据类型
  override def getProducedType: TypeInformation[UserLogPageViewData] = {
    return TypeInformation.of(classOf[UserLogPageViewData])
  }
}
