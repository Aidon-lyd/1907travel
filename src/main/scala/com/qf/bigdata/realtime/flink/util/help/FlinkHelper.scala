package com.qf.bigdata.realtime.flink.util.help

import java.util.Properties

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealTimeDimDO.ProductDimDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, QBase}
import com.qf.bigdata.realtime.flink.streaming.rdo.typeinfomation.QRealTimeDimTypeInfomation
import com.qf.bigdata.realtime.flink.util.es.ESConfigUtil
import com.qf.bigdata.realtime.flink.util.es.ESConfigUtil.ESConfigHttpHost
import com.qf.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}

/**
 * Flink辅助工具类
 */
object FlinkHelper {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger("FlinkHelper")

  //默认重启次数
  val DEF_FLINK_RESTART_STRATEGY_NUM = 10


  /**
   * 流式环境flink上下文构建
   * @param checkPointInterval 检查点时间间隔
   * @return
   */
  def createStreamingEnvironment(checkPointInterval :Long, tc :TimeCharacteristic, watermarkInterval:Long) :StreamExecutionEnvironment = {
    var env : StreamExecutionEnvironment = null
    try{
      //构建flink批处理上下文对象
      env = StreamExecutionEnvironment.getExecutionEnvironment

      //设置执行并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      //开启checkpoint
      env.enableCheckpointing(checkPointInterval, CheckpointingMode.EXACTLY_ONCE)

      //flink服务重启机制
      /*
      1、针对作业设置的失败重启机制：
      flink服务重启机制：flink作业失败后重启任务的机制
      fixedDelayRestart： 定期间隔重启服务，，重启N次，每隔多久重启1次
      failureRateRestart:故障率重启服务,,failureRate：最大失败率 failureInterval:失败间隔  delayInterval：延迟重启间隔
      noRestart: 不重启，，不重启
      2、可以在flink-conf.yaml中配置：
      restart-strategy.fixed-deply.attcempts = 5
      restart-strategy.fixed-deply.delay=10*1000

      失败率属性：
       */
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(QRealTimeConstant.RESTART_ATTEMPTS,QRealTimeConstant.RESTART_DELAY_BETWEEN_ATTEMPTS))

      //时间语义
      env.setStreamTimeCharacteristic(tc)

      //创建水位时间间隔
      env.getConfig.setAutoWatermarkInterval(watermarkInterval)

    }catch{
      case ex:Exception => {
        println(s"FlinkHelper create flink context occur exception：msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }
    env
  }


  /**
   * flink读取kafka数据
   * @param env flink上下文环境对象
   * @param topic kafka主题
   * @return
   */
  def createKafkaConsumer(env:StreamExecutionEnvironment, topic:String, groupID:String) :FlinkKafkaConsumer[String] = {
    //kafka数据序列化
    val schema = new SimpleStringSchema()

    //kafka消费参数
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费组
    consumerProperties.setProperty("group.id", groupID)

    //创建kafka消费者
    val kafkaConsumer : FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, schema, consumerProperties)

    //flink消费kafka数据策略：读取最新数据
    kafkaConsumer.setStartFromLatest()

    //kafka数据偏移量自动提交（默认为true，配合kafka消费参数 enable.auto.commit=true）
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer
  }


  /**
   * flink读取kafka数据
   * @param env flink上下文环境对象
   * @param topic kafka主题
   * @param sm kafka消费策略
   * @return
   */
  def createKafkaSerDeConsumer[T: TypeInformation](env:StreamExecutionEnvironment, topic:String, groupID:String, schema:KafkaDeserializationSchema[T], sm: StartupMode) :FlinkKafkaConsumer[T] = {
    //kafka消费参数
    val consumerProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费组
    consumerProperties.setProperty("group.id", groupID)

    //创建kafka消费者
    val kafkaConsumer : FlinkKafkaConsumer[T] = new FlinkKafkaConsumer(topic, schema, consumerProperties)

    //flink消费kafka数据策略：读取最新数据
    if(StartupMode.LATEST.equals(sm)){
      kafkaConsumer.setStartFromLatest()
    }else if(StartupMode.EARLIEST.equals(sm)){//flink消费kafka数据策略：读取最早数据
      kafkaConsumer.setStartFromEarliest()
    }

    //kafka数据偏移量自动提交（默认为true，配合kafka消费参数 enable.auto.commit=true）
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

    kafkaConsumer
  }


  /**
   * 事件时间提取(固定有序数据)
   */
  def getBoundedAssigner[T <: QBase]():BoundedOutOfOrdernessTimestampExtractor[T] = {
    val boundedAssigner = new BoundedOutOfOrdernessTimestampExtractor[T](Time.milliseconds(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)) {
      override def extractTimestamp(element: T): Long = {
        element.ct
      }
    }
    boundedAssigner
  }




  /**
   * 参数处理工具
   * @param args 参数列表
   * @return
   */
  def createParameterTool(args: Array[String]):ParameterTool = {
    val parameterTool = ParameterTool.fromArgs(args)
    parameterTool
  }


  /**
   * 创建产品维表数据流
   * @param env flink上下文对象
   * @param sql 查询sql
   * @param fieldTypes 查询对应的列字段
   * @return
   */
  def createProductDimDStream(env: StreamExecutionEnvironment, sql:String,
                              fieldTypes: Seq[TypeInformation[_]]): DataStream[ProductDimDO] = {
    FlinkHelper.createOffLineDataStream(env, sql, fieldTypes).map(
      row => {
        val productID = row.getField(0).toString
        val productLevel = row.getField(1).toString.toInt
        val productType = row.getField(2).toString
        val depCode = row.getField(3).toString
        val desCode = row.getField(4).toString
        val toursimType = row.getField(5).toString
        new ProductDimDO(productID, productLevel, productType, depCode, desCode, toursimType)
      }
    )
  }

  /**
   * 创建jdbc数据源输入格式
   * @param driver jdbc连接驱动
   * @param username jdbc连接用户名
   * @param passwd jdbc连接密码
   * @return
   */
  def createJDBCInputFormat(driver:String, url:String, username:String, passwd:String,
                            sql:String, fieldTypes: Seq[TypeInformation[_]]): JDBCInputFormat = {

    //sql查询语句对应字段类型列表
    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)

    //数据源提取
    val jdbcInputFormat :JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(passwd)
      .setRowTypeInfo(rowTypeInfo)
      .setQuery(sql)
      .finish()
    //返回jdbc的输入格式
    jdbcInputFormat
  }


  /**
   * 维度数据加载
   * @param env flink上下文环境对象
   * @param sql 查询语句
   * @param fieldTypes 查询语句对应字段类型列表
   * @return
   */
  def createOffLineDataStream(env: StreamExecutionEnvironment, sql:String, fieldTypes: Seq[TypeInformation[_]]):DataStream[Row] = {
    //JDBC属性
    val mysqlDBProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.MYSQL_CONFIG_URL)

    //flink-jdbc包支持的jdbc输入格式
    val jdbcInputFormat : JDBCInputFormat= FlinkHelper.createJDBCInputFormat(mysqlDBProperties, sql, fieldTypes)

    //jdbc数据读取后形成数据流[其中Row为数据类型，类似jdbc]
    val jdbcDataStream :DataStream[Row] = env.createInput(jdbcInputFormat)
    //数据流
    jdbcDataStream
  }



  /**
   * 创建jdbc数据源输入格式
   * @param properties jdbc连接参数
   * @param sql 查询语句
   * @param fieldTypes 查询语句对应字段类型列表
   * @return
   */
  def createJDBCInputFormat(properties:Properties, sql:String, fieldTypes: Seq[TypeInformation[_]]): JDBCInputFormat = {
    //jdbc连接参数
    val driver :String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
    val url :String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_URL_KEY)
    val user:String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_USERNAME_KEY)
    val passwd:String = properties.getProperty(QRealTimeConstant.FLINK_JDBC_PASSWD_KEY)

    //flink-jdbc包支持的jdbc输入格式
    val jdbcInputFormat : JDBCInputFormat = createJDBCInputFormat(driver, url, user, passwd,
      sql, fieldTypes)

    jdbcInputFormat
  }

  /**
   * ES集群地址
   * @return
   */
  def getESCluster() : ESConfigHttpHost = {
    ESConfigUtil.getConfigHttpHost(QRealTimeConstant.ES_CONFIG_PATH)
  }


  /**
   * 字符拼凑
   * @param sep 拼接符号
   * @param params 拼接数据(类似可变数组)
   * @return
   */
  def concat(sep:String, params:String*):String ={
    val paramCount = params.length
    var index = 1
    val result :StringBuffer = new StringBuffer()
    for(param <- params){
      if(index != paramCount){
        result.append(param).append(sep)
      }else{
        result.append(param)
      }
    }
    result.toString
  }


  /**
   * 旅游产品实时明细数据流
   * @param groupID 消费分组
   * @param fromTopic kafka消费主题
   * @return
   */
  def createOrderDetailDStream(env: StreamExecutionEnvironment, groupID:String, fromTopic:String, timeChar :TimeCharacteristic):DataStream[OrderDetailData] ={
    /**
     * kafka流式数据源
     * kafka消费配置参数
     * kafka消费策略
     */
    val kafkaConsumer : FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupID)


    /**
     * 旅游产品订单数据
     *   (1) kafka数据源(原始明细数据)->转换操作
     *   (2) 设置执行任务并行度
     *   (3) 设置水位及事件时间(如果时间语义为事件时间)
     */
    //固定范围的水位指定(注意时间单位)
    var orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
      .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
      .map(new OrderDetailDataMapFun())
    if(TimeCharacteristic.EventTime.equals(timeChar)){
      val ordersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      orderDetailDStream  = orderDetailDStream.assignTimestampsAndWatermarks(ordersPeriodicAssigner)
    }
    orderDetailDStream
  }

  /**
   * redis连接参数(单点)
   */
  def createRedisConfig(db:Int) : FlinkJedisConfigBase = {

    //redis配置文件
    val redisProperties :Properties = PropertyUtil.readProperties(QRealTimeConstant.REDIS_CONF_PATH)

    //redis连接参数
    var redisDB :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_DB).toInt
    if(null != db){
      redisDB = db
    }

    val redisMaxIdle :Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXIDLE).toInt
    val redisMinIdle:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MINIDLE).toInt
    val redisMaxTotal:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_MAXTOTAL).toInt
    val redisHost:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_HOST)
    val redisPassword:String = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PASSWORD)
    val redisPort:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_PORT).toInt
    val redisTimeout:Int = redisProperties.getProperty(QRealTimeConstant.REDIS_CONF_TIMEOUT).toInt

    //redis配置对象构造
    new FlinkJedisPoolConfig.Builder()
      .setHost(redisHost)
      .setPort(redisPort)
      .setPassword(redisPassword)
      .setTimeout(redisTimeout)
      .setDatabase(redisDB)
      .setMaxIdle(redisMaxIdle)
      .setMinIdle(redisMinIdle)
      .setMaxTotal(redisMaxTotal)
      .build
  }


  def main(args: Array[String]): Unit = {
    /*
    //测试flink的执行环境变量
    println(createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.EventTime,QRealTimeConstant.FLINK_WATERMARK_INTERVAL))*/

    val env: StreamExecutionEnvironment = createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.EventTime, QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

    //测试flink-jdbc的连接器
    /*val res: DataStream[Row] = createOffLineDataStream(env, QRealTimeConstant.SQL_PRODUCT, QRealTimeDimTypeInfomation.getProductDimFieldTypeInfos())
    res.print("flink-jdbc的结果集->")*/

    //获取产品维度信息
    /*createProductDimDStream(env, QRealTimeConstant.SQL_PRODUCT, QRealTimeDimTypeInfomation.getProductDimFieldTypeInfos())
      .print("flink-jdbc获取维度数据->")*/

    //kafka的消费测试
    val res: DataStream[String] = env.addSource(createKafkaConsumer(env, "travel_logs_ods", "logs-group-id1"))
    res.print("flink-kafka数据->")

    //触发执行
    env.execute("product")
  }
}
