package com.qf.bigdata.realtime.flink.streaming.etl.ods

import java.util.Properties

import com.qf.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.{UserLogViewListAssigner}
import com.qf.bigdata.realtime.flink.streaming.funs.logs.{UserLogsViewListFlatMapFunc}
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{ UserLogViewListData}
import com.qf.bigdata.realtime.flink.streaming.schema.{UserLogsViewListFactSchema, UserLogsViewListSchema}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 将用户行为的产品浏览列表数据实时ETL，然后并存储到kafka中
 *
 */
object UserLogsViewListHandler {
  //日志打印
  private val logger: Logger = LoggerFactory.getLogger("UserLogsViewHandler")

  /**
   * 过滤出产品浏览列表数据---打入kafka
   * pro1
   * "exts": "{
   * "travelSendTime":"202002",
   * "travelTime":"9",
   * "productLevel":"4",
   * "targetIDS":"["P40","P46","P28","P62"]",
   * "travelSend":"520381",
   * "productType":"01"
   * }"
   *
   * pro1 P40
   * pro1 p46
   * pro p28
   *
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
    val schema: UserLogsViewListSchema = new UserLogsViewListSchema(fromTopic)
    val userLog_kafkaConsumer: FlinkKafkaConsumer[QRealtimeDO.UserLogViewListData] = FlinkHelper
      .createKafkaSerDeConsumer(env, fromTopic, groupId, schema, StartupMode.LATEST)

    val userLogsAssinger: UserLogViewListAssigner = new UserLogViewListAssigner(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)
    //读取kafka中的数据
    import org.apache.flink.api.scala._
    val userLogViewListDStream: DataStream[QRealtimeDO.UserLogViewListFactData] = env.addSource(userLog_kafkaConsumer)
      .setParallelism(4)
      .assignTimestampsAndWatermarks(userLogsAssinger)
      //过滤出action为05交互的行为 --- eventtype为view\slide
      //需要枚举
      .filter((log: UserLogViewListData) => {
        (log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode)
          && EventEnum.getViewListEvents.contains(log.eventType))
      })
      //映射将userLogViewListData中的ext数据提取出来 -- 转换成UserLogViewListFactData
      .flatMap(new UserLogsViewListFlatMapFunc)

    /*
    将数据打入kafka中----序列化到kakfa中
     */
    //获取kafka的生产者配置信息
    val kafkaProducerConfig: Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
    //获取kafka的schema信息---数据序列化
    val viewSchema: UserLogsViewListFactSchema = new UserLogsViewListFactSchema(toTopic)

    //获取kafka的生产者
    val flinkKafkaProducer: FlinkKafkaProducer[QRealtimeDO.UserLogViewListFactData] = new FlinkKafkaProducer(
      toTopic,
      viewSchema,
      kafkaProducerConfig,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )
    //写入时间戳
    flinkKafkaProducer.setWriteTimestampToKafka(true)
    //将生产者添加到sink
    userLogViewListDStream.addSink(flinkKafkaProducer)
    userLogViewListDStream.print("打入kafka明细数据->")
    //触发执行
    env.execute(appName)
  }

  def main(args: Array[String]): Unit = {
    //打入kafka测试
    handleUserLogs2Kafka("userLogClick2ES","travel_logs_ods","log_groupid_1","travel_logs_viewList")
  }
}