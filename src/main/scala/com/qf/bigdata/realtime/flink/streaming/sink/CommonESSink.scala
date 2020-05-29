package com.qf.bigdata.realtime.flink.streaming.sink

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}


/**
  * 自定义ES Sink(数据元素以json字符串形式可以通用)
  * 明细数据供使用方查询
  */
class CommonESSink(indexName:String) extends RichSinkFunction[String] {

  //日志记录
  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  //ES客户端连接对象
  var transportClient: PreBuiltTransportClient = _

  /**
    * 初始化：连接ES集群
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    //flink与es网络通信参数设置(默认虚核)
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    transportClient = ES6ClientUtil.buildTransportClient()
    super.open(parameters)
  }


  /**
    * Sink输出处理
    * @param value 数据
    * @param context 函数上下文环境对象
    */
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val checkResult: String = checkData(value)
      if (StringUtils.isNotBlank(checkResult)) {
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }

      //将数据写入ES集群
      handleData(indexName, indexName, value)
    }catch{
      case ex: Exception => logger.error(ex.getMessage)
    }
  }


  /**
    * ES插入或更新数据
    * @param idxName 索引名称
    * @param idxTypeName 索引类型名
    */
  def handleData(idxName :String, idxTypeName :String, data:String): Unit ={

    //数据转换
    val record :java.util.Map[String,Object] = JsonUtil.json2object(data, classOf[java.util.Map[String,Object]])

    //esID(用做ES的索引ID)
    val eid :String = record.get(QRealTimeConstant.KEY_ES_ID).toString

    //es索引操作对象
    val indexRequest = new IndexRequest(idxName, idxName, eid).source(record)
    val response = transportClient.prepareUpdate(idxName, idxName, eid)
      //允许写入数据(id重复)时发送冲突的次数
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      //写入索引数据
      .setDoc(record)
      //插入或更新操作
      .setUpsert(indexRequest)
      //执行写入操作结果
      .get()

    //写入异常处理
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      logger.error("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(record))
      throw new Exception("run exception:status:" + response.status().name())
    }
  }

  /**
    * 资源关闭
    */
  override def close() = {
    if (this.transportClient != null) {
      this.transportClient.close()
    }
  }

  /**
    * 参数校验(由于匹配通用数据只能检测共性数据)
    * @param value
    * @return
    */
  def checkData(value: String): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //转换为Map结构
    val record :java.util.Map[String,Object] = JsonUtil.gJson2Map(value)

    //索引id
    val id = record.get(QRealTimeConstant.KEY_ES_ID).toString
    if(null == id){
      msg = "Travel.ESSink.id  is null"
    }
    msg
  }
}
