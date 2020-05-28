package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.UserLogsAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.logs.{UserLogClickDataFunc, UserLogViewDataMapFunc}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.UserLogData
import com.qf.bigdata.realtime.flink.streaming.schema.{UserLogsPageViewSchema, UserLogsSchema}
import com.qf.bigdata.realtime.flink.streaming.sink.logs.{UserLogClickESSink, UserLogViewESSink}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.slf4j.{Logger, LoggerFactory}

/**
 * 将用户行为的页面浏览数实时ETL，然后并存储到es和kafka中
 * 主要是action为：07和08
 */
object UserLogsViewHandler {
  //日志打印
  private val logger: Logger = LoggerFactory.getLogger("UserLogsViewHandler")

  /**
   * 过滤出浏览页面的行为明细数据
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
    val userLogViewDStream: DataStream[QRealtimeDO.UserLogPageViewData] = env.addSource(userLog_kafkaConsumer)
      .setParallelism(4)
      .assignTimestampsAndWatermarks(userLogsAssinger)
      //过滤出action为07和08的数据
      //需要枚举
      .filter((log: UserLogData) => {
        (log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode)
          && log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode))
      })
      //映射将userLogData中的ext数据提取出来
      .map(new UserLogViewDataMapFunc)

    //编写ES的sink，，将结果数据打入es中即可
    userLogViewDStream.addSink(new UserLogViewESSink(QRealTimeConstant.ES_INDEX_NAME_LOG_CLICK))
    userLogViewDStream.print()
    //触发执行
    env.execute(appName)
  }



  /**
   * 过滤出浏览页面的行为明细数据---打入kafka
   * @param appName
   * @param fromTopic
   * @param groupId
   * @param toTopic
   */
  def handleUserLogs2Kafka(appName:String,fromTopic:String,groupId:String,toTopic:String): Unit ={
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
    val userLogViewDStream: DataStream[QRealtimeDO.UserLogPageViewData] = env.addSource(userLog_kafkaConsumer)
      .setParallelism(4)
      .assignTimestampsAndWatermarks(userLogsAssinger)
      //过滤出action为07和08的数据
      //需要枚举
      .filter((log: UserLogData) => {
        (log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode)
          || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode))
      })
      //映射将userLogData中的ext数据提取出来
      .map(new UserLogViewDataMapFunc)

    /*
    将数据打入kafka中----序列化到kakfa中
     */
    //获取kafka的生产者配置信息
    val kafkaProducerConfig: Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
    //获取kafka的schema信息---数据序列化
    val viewSchema: UserLogsPageViewSchema = new UserLogsPageViewSchema(toTopic)

    //获取kafka的生产者
    val flinkKafkaProducer: FlinkKafkaProducer[QRealtimeDO.UserLogPageViewData] = new FlinkKafkaProducer(
      toTopic,
      viewSchema,
      kafkaProducerConfig,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )
    //写入时间戳
    flinkKafkaProducer.setWriteTimestampToKafka(true)
    //将生产者添加到sink
    userLogViewDStream.addSink(flinkKafkaProducer)
    userLogViewDStream.print("打入kafka明细数据->")
    //触发执行
    env.execute(appName)
  }

  def main(args: Array[String]): Unit = {
    //打入es测试
    //handleUserLogs2ES("userLogClick2ES","travel_logs_ods","log_groupid_1",QRealTimeConstant.ES_INDEX_NAME_LOG_CLICK)
    //打入kafka测试
    handleUserLogs2Kafka("userLogClick2ES","travel_logs_ods","log_groupid_1","travel_logs_pageview")
  }
}