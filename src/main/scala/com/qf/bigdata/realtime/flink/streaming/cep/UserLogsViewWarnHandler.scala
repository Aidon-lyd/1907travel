package com.qf.bigdata.realtime.flink.streaming.cep

import com.qf.bigdata.realtime.enumes.ActionEnum
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.UserLogsAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.cep.UserLogsCepFun.UserLogsViewPatternProcessFun
import com.qf.bigdata.realtime.flink.streaming.funs.logs.UserLogViewDataMapFunc
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogData, UserLogPageViewAlertData, UserLogPageViewData}
import com.qf.bigdata.realtime.flink.streaming.schema.{UserLogsSchema, UserLogsViewAlertKSchema}
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema}
import org.slf4j.{Logger, LoggerFactory}


/**
 * 需求：
  * 用户行为日志
  * 业务：基于页面浏览异行为中的【停留时长】异常报警处理
  * 技术：基于flink cep
 *
 * 扩展1：
 * 启动日志报警处理
 * 假设规则：N分钟内启动M次，，，
 *
 * 扩展2：
 * 订单异常处理
 * 假设规则：N分钟连续下单Mci ，，，刷单行为
 *
 * 扩展3：
 * 登录异常处理
 * 假设规则：N分钟连续异地登录M次 ，，，盗登
 *
 *扩展4：
 * 异常获取积分
 * 假设规则：N分钟连续通过M种手段的次数 ，，，
 *
 * 扩展5：
 * 交易异常
 * 假设规则：交易中价值低于实际价值1次
  */
object UserLogsViewWarnHandler {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("UserLogsViewWarnHandler")

  /**
    *
    */
  def handleViewWarnJob(appName:String, groupID:String, fromTopic:String, toTopic:String, timeRange:Int, times:Int, minDuration:Long, maxDuration:Long):Unit = {
    /**
      * 1 Flink环境初始化
      *   流式处理的时间特征依赖(使用事件时间)
      */
    //注意：检查点时间间隔单位：毫秒
    val checkpointInterval = QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL
    val tc = TimeCharacteristic.EventTime
    val watermarkInterval= QRealTimeConstant.FLINK_WATERMARK_INTERVAL
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(checkpointInterval, tc, watermarkInterval)


    /**
      * 2 kafka流式数据源
      *   kafka消费配置参数
      *   kafka消费策略
      *   创建flink消费对象FlinkKafkaConsumer
      *   用户行为日志(kafka数据)反序列化处理
      */
    val schema:KafkaDeserializationSchema[UserLogData] = new UserLogsSchema(fromTopic)
    val kafkaConsumer : FlinkKafkaConsumer[UserLogData] = FlinkHelper.createKafkaSerDeConsumer(env, fromTopic, groupID, schema, StartupMode.LATEST)

    /**
      * 3 创建页面浏览日志数据流
      *   (1)设置处理时间的时间语义
      *   (2) 数据过滤
      *   (3) 数据转换
      */
    val userLogsPeriodicAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
    val viewDStream :DataStream[UserLogPageViewData] = env.addSource(kafkaConsumer)
      .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      .assignTimestampsAndWatermarks(userLogsPeriodicAssigner)
      //过滤action为07和08的数据
      .filter(
        (log : UserLogData) => {
          log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode) || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode)
        }
      )
      .map(new UserLogViewDataMapFunc())


    /**
      * 4 设置复杂规则 cep
      *   规则：5分钟内连续连续 停留时长 小于X 大于Y 的情况出现3次以上
     *
     *   ---pattern导入scala包
      */
    val pattern :Pattern[UserLogPageViewData, UserLogPageViewData] =Pattern
      .begin[UserLogPageViewData](QRealTimeConstant.FLINK_CEP_VIEW_BEGIN)
      .where(
        //对应规则逻辑
        (value: UserLogPageViewData) => {
          val durationTime = value.duration.toLong
          durationTime < minDuration || durationTime > maxDuration
          }
        )
        .timesOrMore(times)//匹配规则次数--3
        .consecutive() //连续匹配模式
        .within(Time.minutes(timeRange))  //多少时间范围以内

    /**
      * 页面浏览告警数据流
      */
    val viewPatternStream :PatternStream[UserLogPageViewData]= CEP.pattern(viewDStream, pattern)
    //获取满足条件的数据流
    val viewDurationAlertDStream :DataStream[UserLogPageViewAlertData] = viewPatternStream.process(
      new UserLogsViewPatternProcessFun()
    )

    //5 写入下游环节
    val kafkaSerSchema = new UserLogsViewAlertKSchema(toTopic)
    val kafkaProductConfig = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
    val travelKafkaProducer = new FlinkKafkaProducer(
      toTopic,
      kafkaSerSchema,
      kafkaProductConfig,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)

    // 加入kafka摄入时间
    travelKafkaProducer.setWriteTimestampToKafka(true)
    viewDurationAlertDStream.addSink(travelKafkaProducer)
    viewDurationAlertDStream.print("浏览异常告警数据流->")

    env.execute(appName)
  }

  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "qf.UserLogsViewWarnHandler"
    //kafka消费组
    val groupID = "group.UserLogsViewWarnHandler"

    //kafka数据源topic
    //val fromTopic = QRealTimeConstant.TOPIC_LOG_ODS
    val fromTopic = "travel_logs_ods"

    //告警输出通道
    //val toTopic = QRealTimeConstant.TOPIC_LOG_ACTION_LAUNCH_WARN
    val toTopic = "travel_dm_logs_warn"

    //规则涉及参数：
    val timeRange :Int = 5 //报警设置的时间周期范围
    val times :Int = 3     //报警设置的匹配次数
    val minDuration:Long = 5l //报警设置的匹配逻辑(规则：阈值下限，最小【停留时长】)
    val maxDuration:Long = 50l//规则设置的匹配逻辑(规则：阈值上限，最大【停留时长】)

    //页面浏览日志规则告警
    handleViewWarnJob(appName, groupID, fromTopic, toTopic, timeRange, times, minDuration, maxDuration)
  }
}
