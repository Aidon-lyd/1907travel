package com.qf.bigdata.realtime.flink.streaming.etl.ods

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.UserLogsAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogClickDataFunc
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.UserLogData
import com.qf.bigdata.realtime.flink.streaming.schema.UserLogsSchema
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.slf4j.{Logger, LoggerFactory}

/**
 * 将用户行为的点击的明细数据打入es中
 */
object UserLogsClickHandler {
  //日志打印
  private val logger: Logger = LoggerFactory.getLogger("UserLogsClinkHandler")

  /**
   * 过滤出点击行为中的点击事件明细数据
   * @param appName
   * @param fromTopic
   * @param groupId
   * @param indexName
   */
  def handleUserLogs2ES(appName:String,fromTopic:String,groupId:String,indexName:String): Unit ={
    //获取执行环境
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.ProcessingTime,
      QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

    //获取kafka的topic连接器---使用带schema的序列化和反序列化
    import org.apache.flink.api.scala._
    val schema: UserLogsSchema = new UserLogsSchema(fromTopic)
    val userLog_kafkaConsumer: FlinkKafkaConsumer[QRealtimeDO.UserLogData] = FlinkHelper
      .createKafkaSerDeConsumer(env, fromTopic, groupId, schema, StartupMode.LATEST)

    val userLogsAssinger: UserLogsAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)
    //读取kafka中的数据
    import org.apache.flink.api.scala._
    val userLogClickDataDStream: DataStream[QRealtimeDO.UserLogClickData] = env.addSource(userLog_kafkaConsumer)
      .setParallelism(4)
      .assignTimestampsAndWatermarks(userLogsAssinger)
      //过滤出交互行为，，事件为点击事件
      //需要枚举
      .filter((log: UserLogData) => {
        (log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode)
          && log.eventType.equalsIgnoreCase(EventEnum.CLICK.getCode))
      })
      //映射将userLogData中的ext数据提取出来
      .map(new UserLogClickDataFunc)

    //编写ES的sink，，将结果数据打入es中即可

  }
}
