package com.qf.bigdata.realtime.flink.streaming.sink.orders

import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderWideData
import com.qf.bigdata.realtime.flink.util.es.ES6ClientUtil
import com.qf.bigdata.realtime.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 自定义ES Sink(数据元素以json字符串形式可以通用)
  * 订单宽表明细数据供使用方查询
  */
class OrdersWideAggESSink(indexName:String) extends RichSinkFunction[OrderWideData]{

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
  override def invoke(value: OrderWideData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val orderDataJson:String = JsonUtil.gObject2Json(value)
      val checkResult: String = checkData(value)
      if (StringUtils.isNotBlank(checkResult)) {
        logger.error("Travel.order.ESRecord.sink.checkData.err{}", checkResult)
        return
      }

      //用户地区
      val depCode = value.depCode
      val id = depCode

      //确定处理维度及度量
      val useFields :List[String] = getOrderWideAggUseFields()
      val sources :java.util.Map[String,Object] = JsonUtil.json2object(orderDataJson, classOf[java.util.Map[String,Object]])
      //提取使用字段
      val record :java.util.Map[String,Object] = checkUseFields(sources, useFields)

      val feeIntValue :Int = record.getOrDefault(QRealTimeConstant.POJO_FIELD_FEE,"").toString.toInt

      //由于gson里将数字转为string，这里fee字段的增加需要转为数字类型
      record.put(QRealTimeConstant.POJO_FIELD_FEE, java.lang.Integer.valueOf(feeIntValue))
      record.put(QRealTimeConstant.POJO_FIELD_ORDERS, java.lang.Long.valueOf(QRealTimeConstant.COMMON_NUMBER_ONE))

      //将数据写入ES集群(使用ES局部更新功能累计度量数据)
      handleData(indexName, indexName, id, record)

    }catch{
      case ex: Exception => logger.error(ex.getMessage)
    }
  }

  /**
    * ES插入或更新数据
    * @param idxName 索引名称
    * @param idxTypeName 索引类型名
    * @param esID 索引ID
    * @param value 写入数据
    */
  def handleData(idxName :String, idxTypeName :String, esID :String,
                 value: mutable.Map[String,Object]): Unit ={
    //脚本参数赋值
    val params = new java.util.HashMap[String, Object]
    val scriptSb: StringBuilder = new StringBuilder
    for ((k :String, v:Object) <- value ) { //if(null != k); if(null != v)
      params.put(k, v)
      var s = ""
      if(QRealTimeConstant.KEY_CT.equals(k)) {
        //订单产生时间
        s = "if(params."+k+" != null){ctx._source."+k+" = params."+k+"} "
      }else if(QRealTimeConstant.POJO_FIELD_FEE.equalsIgnoreCase(k)){
        //订单累计费用
        val feeValue = v.toString.toInt
        s = " if(ctx._source."+k+" != null){ctx._source."+k +" += " + feeValue + "} else { ctx._source."+k+" = "+feeValue+"} "
      }else if(QRealTimeConstant.POJO_FIELD_ORDERS.equalsIgnoreCase(k)){
        //订单PV
        s = " if(ctx._source."+k+" == null){ctx._source."+k+" = 1 } else { ctx._source."+k+" += 1 }"
      }
      scriptSb.append(s)
    }

    //执行脚本
    val scripts = scriptSb.toString()
    val script = new Script(ScriptType.INLINE, "painless", scripts, params)
    println(s"script=$script")
    //logger.info(scripts)

    //ES执行插入或更新操作
    val indexRequest = new IndexRequest(idxName, idxTypeName, esID).source(params)
    val response = transportClient.prepareUpdate(idxName, idxTypeName, esID)
      .setScript(script)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setUpsert(indexRequest)
      .get()
    //写入异常处理
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      logger.error("calculate es session record error!scripts:" + scripts)
      throw new Exception("run exception:status:" + response.status().name())
    }
  }



  /**
    * 选择参与计算的维度和度量
    * @return
    */
  def getOrderWideAggUseFields() : List[String] = {
    var useFields :List[String] = List[String]()
    //useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_USERREGION)
    //useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_TRAFFIC)
    //useFields = useFields.:+(QRealTimeConstant.POJO_PRODUCT_DEPCODE)

    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_ORDERS)
    useFields = useFields.:+(QRealTimeConstant.POJO_FIELD_FEE)
    useFields = useFields.:+(QRealTimeConstant.KEY_CT)

    useFields
  }


  /**
    * 选择参与计算的维度和度量
    * @return
    */
  def checkUseFields(datas:java.util.Map[String,Object], useFields:java.util.List[String]) : java.util.Map[String,Object] = {
    val result :java.util.Map[String,Object] = new java.util.HashMap[String,Object]
    for(field <- useFields){
      if(datas.containsKey(field)){
        val value = datas.getOrDefault(field,null)
        result.put(field, value)
      }
    }
    result
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
    * 参数校验
    * @param value
    * @return
    */
  def checkData(value: OrderWideData): String ={
    var msg = ""
    if(0 == value){
      msg = "kafka.value is empty"
    }

    //产品ID
    val productID = value.productID
    if(0 == productID){
      msg = "Travel.order.ESSink.productID  is null"
    }

    //出行交通
    val triffic = value.traffic
    if(triffic == 0){
      msg = "Travel.order.ESSink.triffic  is null"
    }

    //时间
    val ctNode = value.ct
    if(ctNode == 0){
      msg = "Travel.order.ESSink.ct is null"
    }

    msg
  }


}
