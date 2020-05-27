package com.qf.bigdata.realtime.flink.util.es

import java.net.{InetAddress, InetSocketAddress}

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.HttpHost


/**
  * es配置文件加载工具
  */
object ESConfigUtil {

  var esConfigSocket: ESConfigSocket = null
  var esConfigHttpHost : ESConfigHttpHost = null

  //封装socket的连接配置方式
  class ESConfigSocket(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[InetSocketAddress])
  //封装http的连接配置方式
  class ESConfigHttpHost(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[HttpHost])

  /**
    * 客户端连接
    * @param configPath
    * @return
    */
  def getConfigSocket(configPath: String): ESConfigSocket = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == esConfigSocket) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")

      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      val addressJsonNode = configJsonObject.get("address")
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses = {
        val transportAddresses = new java.util.ArrayList[InetSocketAddress]
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          transportAddresses.add(new InetSocketAddress(InetAddress.getByName(ip), port))
        }
        transportAddresses
      }

      esConfigSocket = new ESConfigSocket(config, transportAddresses)
    }
    esConfigSocket
  }


  /**
    *
    * @param configPath
    * @return
    */
  def getConfigHttpHost(configPath: String): ESConfigHttpHost = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == esConfigHttpHost) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")

      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      val addressJsonNode = configJsonObject.get("address")
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses = {
        val httpHosts = new java.util.ArrayList[HttpHost]
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          val schema = "http"

          val httpHost = new HttpHost(ip, port, schema)
          httpHosts.add(httpHost)
        }
        httpHosts
      }

      esConfigHttpHost = new ESConfigHttpHost(config, transportAddresses)
    }
    esConfigHttpHost
  }

  //测试
  def main(args: Array[String]): Unit = {
    print(getConfigSocket("es/es-config.json").transportAddresses.get(0).getAddress)
  }
}
