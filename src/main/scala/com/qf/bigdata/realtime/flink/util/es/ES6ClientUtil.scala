package com.qf.bigdata.realtime.flink.util.es

import java.net.InetSocketAddress
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.util.es.ESConfigUtil.ESConfigSocket
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * es工具类
  */
object ES6ClientUtil {

  val logger :Logger = LoggerFactory.getLogger(ES6ClientUtil.getClass)

  //es连接客户端对象返回
  def buildTransportClient(esConfigPath: String = QRealTimeConstant.ES_CONFIG_PATH): PreBuiltTransportClient = {
    if (null == esConfigPath) {
      throw new RuntimeException("esConfigPath is null!")
    }
    val esConfig: ESConfigSocket = ESConfigUtil.getConfigSocket(esConfigPath)
    val configs : mutable.Map[String,String] = esConfig.config.asScala
    val transAddrs :mutable.Buffer[InetSocketAddress] = esConfig.transportAddresses.asScala

    //es 参数
    val settings :Settings.Builder = Settings.builder()
    for((key, value) <- esConfig.config.asScala){
      settings.put(key, value)
    }

    val transportClient = new PreBuiltTransportClient(settings.build())
    for(transAddr <- transAddrs){
      val transport : TransportAddress = new TransportAddress(transAddr)
      transportClient.addTransportAddress(transport)
    }

    var info = ""
    if (transportClient.connectedNodes.isEmpty) {
      info = "Elasticsearch client is not connected to any Elasticsearch nodes!"
      logger.error(info)
      throw new RuntimeException(info)
    }else {
      info = "Created Elasticsearch TransportClient with connected nodes {}"+ transportClient.connectedNodes
      logger.info(info)
    }
    transportClient
  }

  //测试对es进行写入数据
  def test():Unit = {
    val transportClient = buildTransportClient();

    val idxName = "test";
    val esID = "000000"
    val value = new util.HashMap[String,String]()
    value.put("num","10001")
    value.put("age","10")

    val indexRequest = new IndexRequest(idxName, idxName, esID).source(value)
    val response = transportClient.prepareUpdate(idxName, idxName, esID)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setDoc(value)
      .setUpsert(indexRequest)
      .get()

    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run exception:status:" + response.status().name())
    }

  }

  def main(args: Array[String]): Unit = {
    //buildTransportClient()
    test()
  }
}
