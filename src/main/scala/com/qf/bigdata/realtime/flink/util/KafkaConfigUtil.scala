package com.qf.bigdata.realtime.flink.util

import java.util.Properties
import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode


//获取kafka的配置信息工具类
object KafkaConfigUtil {
  //定义变量
  var config: KafkaConfig = null
  /**
   * 监控配置加载类
   * @param kp  kafka配置
   * @param kts kafka配置对topic集合
   */
  class KafkaConfig(val kp: Properties, val kts: java.util.List[String]) {
  }

  /**
   * 读取kafka配置
   * ----需要引入 jackson-databind:2.9.6
   * @param kafka_config_url
   * @return
   */
  def getConfig(kafka_config_url: String): KafkaConfig = {
    if (config == null) {
      val mapper = new ObjectMapper()

      /*加载kafka配置*/
      val kafka_in = KafkaConfigUtil.getClass.getClassLoader.getResourceAsStream(kafka_config_url)

      /**json顶点*/
      val kafka_tree = mapper.readTree(kafka_in)
      if (kafka_in != null)
        kafka_in.close()

      val kt = kafka_tree.get("KAFKA_TOPICS")
      val kts_arr = classOf[ArrayNode].cast(kt)

      /*生成topic集合*/
      val KAFKA_TOPICS: java.util.List[String] = {
        val set: java.util.List[String] = new util.ArrayList[String]()
        val it = kts_arr.iterator()
        while (it.hasNext) {
          set.add(it.next().asText())
        }
        set
      }

      /*kafka配置*/
      val KAFKA_PROPERTIES: Properties = {
        val p = new Properties()

        val KAFKA_PROPERTIES = kafka_tree.get("KAFKA_PROPERTIES")
        val fieldNames = KAFKA_PROPERTIES.fieldNames()

        while (fieldNames.hasNext) {
          val name = fieldNames.next()
          //循环kafka配置信息，暂时只支持一级节点配置
          p.setProperty(name, KAFKA_PROPERTIES.get(name).asText())
        }
        p
      }
      config = new KafkaConfig(KAFKA_PROPERTIES, KAFKA_TOPICS) //创建配置类信息
    }
    config
  }

  def main(args: Array[String]): Unit = {
    println(getConfig("kafka/kafka-config.json").kp.getProperty("bootstrap.servers"))
  }
}
