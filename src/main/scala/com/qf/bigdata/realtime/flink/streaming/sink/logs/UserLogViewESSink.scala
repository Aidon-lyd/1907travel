package com.qf.bigdata.realtime.flink.streaming.sink.logs

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogClickData, UserLogPageViewData}
import com.qf.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

/*
用户page浏览数据下沉ES中
 */
class UserLogViewESSink(indexName:String) extends RichSinkFunction[UserLogPageViewData]{

  private val logger: Logger = LoggerFactory.getLogger("UserLogViewESSink")

  //transport连接方式
  var transportClient:PreBuiltTransportClient = _

  //负责连接初始化
  override def open(parameters: Configuration): Unit = {
    //获取连接
    transportClient = ES6ClientUtil.buildTransportClient()
  }

  def checkData(res: util.Map[String, AnyRef]): String = {
    //对数据整体是否为空、action、eventtype
    var msg:String = ""
    if(res == null){
      msg = "value is null"
    }
    if(res.get(QRealTimeConstant.KEY_ACTION) == null){
      msg = "action is null"
    }
    if(res.get(QRealTimeConstant.KEY_EVENT_TYPE) == null){
      msg = "evnetType is null"
    }
    if(res.get(QRealTimeConstant.CT) == null){
      msg = "ct is null"
    }
    //返回
    msg
  }

  /**
   * 处理数据存储
   * @param indexName
   * @param sid
   * @param res
   */
  def handleData(indexName: String,indexType: String, sid: String, res: util.Map[String, AnyRef]): Unit = {
    //获取indexRequest对象
    val indexRequest: IndexRequest = new IndexRequest(indexName, indexType, sid).source(res)
    //使用客户端操作数据
    val response: UpdateResponse = transportClient.prepareUpdate(indexName, indexType, sid)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setDoc(res)
      .setUpsert(indexRequest)
      .get()

    println(response.status().getStatus+"++++++++++"+indexName+"-"+indexType+"-"+sid)
    //查看更新状态
    if(response.status() != RestStatus.CREATED && response.status() != RestStatus.OK){
      logger.warn("this data wrtie error,data is:"+new ObjectMapper().writeValueAsString(res))
      //抛异常
      throw new Exception("store data execption=>"+response.status().name())
    }
  }

  //打入es的处理，，每条数据都要执行一次
  override def invoke(value: UserLogPageViewData, context: SinkFunction.Context[_]): Unit = {
    try {
      //将数据转换成map形式
      val res: util.Map[String, AnyRef] = JsonUtil.gObject2Map(value)
      //对res数据做检测
      val checkRes:String = checkData(res)
      //检测数据没有问题就开始存储---如果空直接返回
      if(StringUtils.isNotEmpty(checkRes)){
        logger.warn("userLogViewData is null,data is :",checkRes)
        return
      }
      //继续---需要索引名、id、数据
      val sid = value.sid  //使用于文档id

      //存储 /boook/jisuanji/007
      handleData(indexName,indexName,sid,res)
      print("----------------------------"+sid)
    } catch {
      //case exception:Exception => exception.printStackTrace()
      case exception:Exception => logger.error(exception.getMessage)
    }
  }

  //关闭资源
  override def close(): Unit = {
    if(transportClient != null){
      transportClient.close()
    }
  }
}
