package com.qf.bigdata.realtime.flink.streaming.schema

import java.lang

import com.google.gson.Gson
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.UserLogData
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 用户行为日志数据的序列化和反序列化
 * 1、实现我们的kafka的序列化和反序列化
 */
class UserLogsSchema(topic:String) extends KafkaSerializationSchema[UserLogData]
      with KafkaDeserializationSchema[UserLogData]{
  //序列化
  override def serialize(element: UserLogData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //获取到sid
    val key: String = element.sid
    val value: String = JsonUtil.gObject2Json(element)
    //封装成ProducerRecord
    new ProducerRecord[Array[Byte], Array[Byte]](topic,key.getBytes,value.getBytes)
  }

  //是否结尾
  override def isEndOfStream(nextElement: UserLogData): Boolean = {
    return false
  }

  //反序列化
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogData = {
    //取出key-value
    val value: Array[Byte] = record.value()
    //将我们的value值封装成userLogData对象
    val gson: Gson = new Gson()
    val data: UserLogData = gson.fromJson(new String(value), classOf[UserLogData])
    data
  }

  //获取数据类型
  override def getProducedType: TypeInformation[UserLogData] = {
    return TypeInformation.of(classOf[UserLogData])
  }
}
