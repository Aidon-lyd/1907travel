package com.qf.bigdata.realtime.flink.streaming.agg.mapper

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.QKVBase
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * 自定义实现redis sink的RedisMapper接口
  * 订单聚合结果输出redis
  */
class QRedisSetMapper() extends RedisMapper[QKVBase]{

  /**
    * redis 操作命令
    * @return
    */
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  /**
    * redis key键
    * @param data
    * @return
    */
  override def getKeyFromData(data: QKVBase): String = {
    data.key
  }

  /**
    * redis value
    * @param data
    * @return
    */
  override def getValueFromData(data: QKVBase): String = {
      data.value
  }
}
