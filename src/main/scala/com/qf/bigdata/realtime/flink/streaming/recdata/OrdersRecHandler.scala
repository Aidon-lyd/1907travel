package com.qf.bigdata.realtime.flink.streaming.recdata



import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.assigner.OrdersPeriodicAssigner
import com.qf.bigdata.realtime.flink.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderDetailData
import com.qf.bigdata.realtime.flink.util.help.FlinkHelper
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}


/**
 * 将订单数据落地到hdfs中---> 做离线数仓
 * 1、落入hdfs的普通文件
 * 2、落入hdfs的中parquet格式的文件
 *
 * 数仓事实数据两种文件格式--- parquet、orc
 * 数仓中的维度数据文件格式--- txt
 *
 * 解决流式数据怎么落地？？？
   1、使用代码打入hdfs中，映射hive表 ---按照时间落地，，按照大小落地
 * 2、和hive做整合，然后连接hive进行write
 */
object OrdersRecHandler {

  //日志打印
  private val logger: Logger = LoggerFactory.getLogger("OrdersRecHandler")

  /**
   * 行数据落地hdfs中
   * @param appName  应用名称
   * @param fromTopic 数据来源的topic
   * @param groupId 消费者组
   * @param output 输出到hdfs中的目录
   * @param rolloverInterval 落地间隔
   * @param inactivityInterval 非交互间隔
   * @param maxSize 数据量，，量大小
   * @param bucketCheckInterval 桶检验间隔
   */
  def handleRow2Hdfs(appName:String,fromTopic:String,groupId:String,output:String,
                     rolloverInterval:Long,inactivityInterval:Long,maxSize:Long,
                     bucketCheckInterval:Long)={
    //获取执行环境
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.ProcessingTime,
      QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

    //获取kafka的topic连接器
    val orders_kafkaConsumer: FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupId)

    //读取kafka中的数据
    import org.apache.flink.api.scala._
    val ordersDetileDStream: DataStream[String] = env.addSource(orders_kafkaConsumer)
      .setParallelism(4)   //给source设置并行度
      .map(JsonUtil.object2json(_))  //将json格式的字符串转换成json

    //将ordersDetileDStream流式数据进行落地
    //定义输出路径
    val outputpath: Path = new Path(output)
    //定义数据量大小,,超过该值进行落地
    val maxPartSize:Long = maxSize * 1024
    //落地间隔
    val rolloverInt: Long = TimeUnit.SECONDS.toMillis(rolloverInterval)
    val inactivityInt: Long = TimeUnit.SECONDS.toMillis(inactivityInterval)
    val bucketCheckInt: Long = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

    //数据落地策略
    val rollpolicy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      .withRolloverInterval(rolloverInt)
      .withInactivityInterval(inactivityInt)
      .withMaxPartSize(maxPartSize)
      .build()

    //定义数据的分桶器---时间
    val timeBucketAssigner: DateTimeBucketAssigner[String] = new DateTimeBucketAssigner[String](QRealTimeConstant.FORMATTER_YYYYMMDDHH)

    //定义sink持久化到文件系统中去即可
    val orders_streamingsink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(outputpath, new SimpleStringEncoder[String]("utf-8"))
      .withBucketAssigner(timeBucketAssigner)
      .withRollingPolicy(rollpolicy)
      .withBucketCheckInterval(bucketCheckInt)
      .build()

    //将sink添加到sink中
    ordersDetileDStream.addSink(orders_streamingsink)
    //触发执行
    env.execute(appName)
  }

  /**
   * 将数据落地到hdfs中，数据格式为parquet
   * @param appName  应用名称
   * @param fromTopic 数据来源的topic
   * @param groupId 消费者组
   * @param output 输出到hdfs中的目录
   * @param rolloverInterval 落地间隔
   * @param inactivityInterval 非交互间隔
   * @param maxSize 数据量，，量大小
   * @param bucketCheckInterval 桶检验间隔
   */
  def handleParquet2Hdfs(appName:String,fromTopic:String,groupId:String,output:String,
                     rolloverInterval:Long,inactivityInterval:Long,maxSize:Long,
                     bucketCheckInterval:Long)={
    //获取执行环境
    val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment(QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.ProcessingTime,
      QRealTimeConstant.FLINK_WATERMARK_INTERVAL)

    //获取kafka的topic连接器
    val orders_kafkaConsumer: FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env, fromTopic, groupId)

    //读取kafka中的数据
    import org.apache.flink.api.scala._
    val orderWaterMarkAssginer: OrdersPeriodicAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_INTERVAL)
    val order_detail_DStream: DataStream[QRealtimeDO.OrderDetailData] = env.addSource(orders_kafkaConsumer)
      .setParallelism(4) //给source设置并行度
      .map(new OrderDetailDataMapFun) //将json格式的字符串转换成json
      .assignTimestampsAndWatermarks(orderWaterMarkAssginer)

    //将ordersDetileDStream流式数据进行落地
    //定义输出路径
    val outputpath: Path = new Path(output)
    //定义数据量大小,,超过该值进行落地
    val maxPartSize:Long = maxSize * 1024
    //落地间隔
    val rolloverInt: Long = TimeUnit.SECONDS.toMillis(rolloverInterval)
    val inactivityInt: Long = TimeUnit.SECONDS.toMillis(inactivityInterval)
    val bucketCheckInt: Long = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

    //数据落地策略
    /*val rollpolicy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      .withRolloverInterval(rolloverInt)
      .withInactivityInterval(inactivityInt)
      .withMaxPartSize(maxPartSize)
      .build()*/

    //定义数据的分桶器---时间
    val timeBucketAssigner: DateTimeBucketAssigner[OrderDetailData] = new DateTimeBucketAssigner[OrderDetailData](QRealTimeConstant.FORMATTER_YYYYMMDDHH)

    //---指定parquet格式的数据
    val orders_streamingsink: StreamingFileSink[OrderDetailData] = StreamingFileSink
      //指定parquet---需要引入flink-parquet的依赖
      .forBulkFormat(outputpath, ParquetAvroWriters.forReflectRecord(classOf[OrderDetailData]))
      .withBucketAssigner(timeBucketAssigner)
      .withBucketCheckInterval(bucketCheckInt)
      .build()

    //将sink添加到sink中
    order_detail_DStream.addSink(orders_streamingsink)
    //触发执行
    env.execute(appName)
  }

  def main(args: Array[String]): Unit = {

    //行数据输出测试
    handleRow2Hdfs("order2hdfs",
      "travel_orders_ods",
      "order-group-id",
      "hdfs://hadoop01:9000/travle/orders/",
      60,
      60,
      128,
      60)
  }
}
