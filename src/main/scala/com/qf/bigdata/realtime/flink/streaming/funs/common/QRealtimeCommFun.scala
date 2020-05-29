package com.qf.bigdata.realtime.flink.streaming.funs.common

import java.util
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import java.util.Date

import com.google.common.cache.{Cache, CacheBuilder}
import com.lambdaworks.redis.{RedisClient, RedisURI}
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderDetailData, OrderMWideData, OrderWideData}
import com.qf.bigdata.realtime.util.{CommonUtil, DBDruid, JsonUtil, PropertyUtil, RedisCache}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisPool}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable


/**
  * 实时场景公共函数
  */
object QRealtimeCommFun {

  val logger :Logger = LoggerFactory.getLogger("QRealtimeCommFun")


  /**
    * 数据源信息
    *
    * @param table
    * @param schema
    * @param pk
    * @param sql
    */
  case class DBQuery(table:String, schema:String, pk:String, sql:String)

  /**
    * mysql异步读取函数
    */
  class DimProductAsyncFunction(dbPath:String, dbQuery:DBQuery, useLocalCache:Boolean) extends RichAsyncFunction[OrderDetailData,OrderWideData] {

    //mysql连接
    var pool :DBDruid = _

    //redis连接参数
    val redisIP = "hadoop01"
    val redisPort = 6379
    val redisPass = "root"

    //客户端连接
    var redisClient : RedisClient = _
    var redisConn : StatefulRedisConnection[String,String] = _
    var redisCmd: RedisCommands[String, String] = _

    //本地缓存
    var localCache: Cache[String, String] = _

    //定时调度
    var scheduled : ScheduledThreadPoolExecutor = _

    /**
      * 重新加载数据库数据
      */
    def reloadDB():Unit ={
      val dbResult = pool.execSQLJson(dbQuery.sql, dbQuery.schema, dbQuery.pk)
      if(useLocalCache){
        //本地缓存
        localCache.putAll(dbResult)
      }else{
        val key = dbQuery.table
        redisCmd.hmset(key, dbResult)
      }
    }


    /*
     * 初始化
     */
    override def open(parameters: Configuration): Unit = {
       println(s"""MysqlAsyncFunction open.time=${CommonUtil.formatDate4Def(new Date())}""")
       super.open(parameters)

        //数据库配置文件
        val dbProperties = PropertyUtil.readProperties(dbPath)

        val driver = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
        val url = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_URL_KEY)
        val user = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_USERNAME_KEY)
        val passwd = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_PASSWD_KEY)

        //缓存连接
        pool = new DBDruid(driver, url, user, passwd)
        scheduled  = new ScheduledThreadPoolExecutor(2)
        if(useLocalCache){
          localCache = CacheBuilder.newBuilder.maximumSize(10000).expireAfterAccess(60L, TimeUnit.MINUTES).build[String,String]
        }else{
          redisClient = RedisClient.create(RedisURI.create(redisIP, redisPort))
          redisConn = redisClient.connect
          redisCmd = redisConn.sync
          redisCmd.auth(redisPass)
        }
        //数据初始化加载到缓存
        reloadDB()

        //定时更新缓存
        val initialDelay: Long = 0l
        val period :Long = 60l
       scheduled.scheduleAtFixedRate(new Runnable {
         override def run(): Unit = {
           reloadDB()
         }
       }, initialDelay, period, TimeUnit.SECONDS)
    }


    /**
      * 异步执行
      * @param input
      * @param resultFuture
      */
    override def asyncInvoke(input: OrderDetailData, resultFuture: ResultFuture[OrderWideData]): Unit = {
      try {
        println(s"""MysqlAsyncFunction invoke.time=${CommonUtil.formatDate4Def(new Date())}""")
        val orderProductID = input.productID
        var productInfo :String = ""
        if(useLocalCache){
          //----本地缓存---1、如果查不到，读mysql   2、
          productInfo = localCache.getIfPresent(orderProductID)
        }else{
          val key = dbQuery.table
          val dataJson : util.Map[String,String] = redisCmd.hgetall(key)
          productInfo =  dataJson.getOrDefault(orderProductID,"")
        }
        //如果缓存没有数据
        if(StringUtils.isNotEmpty(productInfo)){
          val productRow : util.Map[String,Object] = JsonUtil.json2object(productInfo, classOf[util.Map[String,Object]])

          //product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type
          val productLevel = productRow.get("product_level").toString.toInt
          val productType = productRow.get("product_type").toString
          val depCode = productRow.get("departure_code").toString
          val desCode = productRow.get("des_city_code").toString
          val toursimType = productRow.get("toursim_tickets_type").toString

          val orderWide = OrderWideData(input.orderID, input.userID, orderProductID, input.pubID,
            input.userMobile, input.userRegion, input.traffic, input.trafficGrade, input.trafficType,
            input.price, input.fee, input.hasActivity,
            input.adult, input.yonger, input.baby, input.ct,
            productLevel, productType, toursimType, depCode, desCode)

          val orderWides = List(orderWide)

          resultFuture.complete(orderWides)
        } else {
          val orderWide = OrderWideData(input.orderID, input.userID, orderProductID, input.pubID,
            input.userMobile, input.userRegion, input.traffic, input.trafficGrade, input.trafficType,
            input.price, input.fee, input.hasActivity,
            input.adult, input.yonger, input.baby, input.ct,
            -1, "-1","-1", "-1", "-1")
          val orderWides = List(orderWide)

          resultFuture.complete(orderWides)
        }
      }catch {
        //注意：加入异常处理放置阻塞产生
        case ex: Exception => {
          println(s"""ex=${ex}""")
          logger.error("DimProductAsyncFunction.err:" + ex.getMessage)
          resultFuture.completeExceptionally(ex)
        }
      }
    }


    /**
      * 超时处理
      * @param input
      * @param resultFuture
      */
    override def timeout(input: OrderDetailData, resultFuture: ResultFuture[OrderWideData]): Unit = {
      println(s"""DimProductAsyncFunction timeout.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.timeout(input, resultFuture)
    }

    /**
      * 关闭
      */
    override def close(): Unit = {
      println(s"""DimProductAsyncFunction close.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.close()
      pool.close()
    }

  }


  //===============================================================================================



  /**
    * 产品相关维表异步读取函数(多表)
    */
  class DimProductMAsyncFunction(dbPath:String, dbQuerys:mutable.Map[String,DBQuery]) extends RichAsyncFunction[OrderDetailData,OrderMWideData] {
    //mysql连接
    var pool :DBDruid = _

    //redis连接参数
    val redisIP = "hadoop01"
    val redisPort = 6379
    val redisPass = "root"

    //redis客户端连接
    var redisClient : RedisClient = _
    var redisConn : StatefulRedisConnection[String,String] = _
    var redisCmd: RedisCommands[String, String] = _

    //定时调度
    var scheduled : ScheduledThreadPoolExecutor = _

    /**
      * 重新加载数据库数据
      */
    def reloadDB():Unit ={
      for(entry <- dbQuerys){
        val tableKey = entry._1
        val dbQuery = entry._2
        val dbResult = pool.execSQLJson(dbQuery.sql, dbQuery.schema, dbQuery.pk)
        val key = dbQuery.table
        redisCmd.hmset(key, dbResult)
      }
    }


    /**
      * redis连接初始化
      *
      */
    def initRedisConn():Unit = {
      redisClient = RedisClient.create(RedisURI.create(redisIP, redisPort))
      redisConn = redisClient.connect
      redisCmd = redisConn.sync
      redisCmd.auth(redisPass)
    }


    /*
     * 初始化
     */
    override def open(parameters: Configuration): Unit = {
      //println(s"""MysqlAsyncFunction open.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.open(parameters)

      //数据库配置文件
      val dbProperties = PropertyUtil.readProperties(dbPath)

      val driver = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
      val url = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_URL_KEY)
      val user = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_USERNAME_KEY)
      val passwd = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_PASSWD_KEY)



      //缓存连接
      pool = new DBDruid(driver, url, user, passwd)

      //redis资源连接初始化
      initRedisConn()

      //数据初始化加载到缓存
      reloadDB()


      //定时更新缓存
      //调度资源
      scheduled  = new ScheduledThreadPoolExecutor(2)
      val initialDelay: Long = 0l
      val period :Long = 60l
      scheduled.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          reloadDB()
        }
      }, initialDelay, period, TimeUnit.SECONDS)
    }


    /**
      * 异步执行
      * @param input
      * @param resultFuture
      */
    override def asyncInvoke(input: OrderDetailData, resultFuture: ResultFuture[OrderMWideData]): Unit = {
      try {
        //println(s"""MysqlAsyncFunction invoke.time=${CommonUtil.formatDate4Def(new Date())}""")
        val orderProductID :String = input.productID
        val pubID :String = input.pubID
        val defValue :String = ""

        /**
          * 产品维表相关数据
          * 使用列：product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type
          */
        val productJson :String = redisCmd.hget(QRealTimeConstant.MYDQL_DIM_PRODUCT, orderProductID)
        var productLevel = QRealTimeConstant.COMMON_NUMBER_ZERO_INT
        var productType = defValue
        var depCode = defValue
        var desCode = defValue
        var toursimType = defValue
        if(StringUtils.isNotEmpty(productJson)){
          val productRow : util.Map[String,Object] = JsonUtil.json2object(productJson, classOf[util.Map[String,Object]])
          productLevel = productRow.get("product_level").toString.toInt

          productType = productRow.get("product_type").toString
          depCode = productRow.get("departure_code").toString
          desCode = productRow.get("des_city_code").toString
          toursimType = productRow.get("toursim_tickets_type").toString
        }

        /**
          * 酒店维表相关数据
          */
        val pubJson :String = redisCmd.hget(QRealTimeConstant.MYDQL_DIM_PUB, pubID)
        var pubStar :String = defValue
        var pubGrade :String = defValue
        var isNational :String = defValue
        if(StringUtils.isNotEmpty(pubJson)){
          val pubRow : util.Map[String,Object] = JsonUtil.json2object(pubJson, classOf[util.Map[String,Object]])
          pubStar = pubRow.get("pub_star").toString
          pubGrade = pubRow.get("pub_grade").toString
          isNational = pubRow.get("is_national").toString
        }

        var orderWide = new OrderMWideData(input.orderID, input.userID, orderProductID, input.pubID,
          input.userMobile, input.userRegion, input.traffic, input.trafficGrade, input.trafficType,
          input.price, input.fee, input.hasActivity,
          input.adult, input.yonger, input.baby, input.ct,
          productLevel, productType, toursimType, depCode, desCode,
          pubStar, pubGrade, isNational)

        val orderWides = List(orderWide)
        resultFuture.complete(orderWides)
      }catch {
        //注意：加入异常处理放置阻塞产生
        case ex: Exception => {
          println(s"""ex=${ex}""")
          logger.error("DimProductMAsyncFunction.err:" + ex.getMessage)
          resultFuture.completeExceptionally(ex)
        }
      }
    }


    /**
      * 超时处理
      * @param input
      * @param resultFuture
      */
    override def timeout(input: OrderDetailData, resultFuture: ResultFuture[OrderMWideData]): Unit = {
      println(s"""DimProductMAsyncFunction timeout.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.timeout(input, resultFuture)
    }

    /**
      * 关闭
      */
    override def close(): Unit = {
      println(s"""DimProductMAsyncFunction close.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.close()
      pool.close()
    }

  }


  /**
    * 产品相关维表异步读取函数(多表)
    */
  class DimProductMAsyncFunction2(dbPath:String, dbQuerys:mutable.Map[String,DBQuery]) extends RichAsyncFunction[OrderDetailData,OrderMWideData] {

    var pool :DBDruid = _
    val redisIP = "hadoop01"
    val redisPort = 6379
    val redisPass = "root"
    var jedisPool : JedisPool = _
    var jedis : Jedis = _
    var scheduled : ScheduledThreadPoolExecutor = _



    /**
      * 重新加载数据库数据
      */
    def reloadDB():Unit ={
      for(entry <- dbQuerys){
        val tableKey = entry._1
        val dbQuery = entry._2
        val dbResult = pool.execSQLJson(dbQuery.sql, dbQuery.schema, dbQuery.pk)
        val key = dbQuery.table
        jedis.hmset(key, dbResult)
      }
    }


    /**
      * redis连接初始化
      *
      */
    def initRedisConn():Unit = {
      val cache = new RedisCache
      jedisPool = cache.connectRedisPool(redisIP)
      jedis = jedisPool.getResource
      jedis.auth(redisPass)
      jedis.select(QRealTimeConstant.REDIS_DB)
    }


    /*
     * 初始化
     */
    override def open(parameters: Configuration): Unit = {
      //println(s"""MysqlAsyncFunction open.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.open(parameters)

      //数据库配置文件
      val dbProperties = PropertyUtil.readProperties(dbPath)

      val driver = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_DRIVER_MYSQL_KEY)
      val url = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_URL_KEY)
      val user = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_USERNAME_KEY)
      val passwd = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_PASSWD_KEY)



      //缓存连接
      pool = new DBDruid(driver, url, user, passwd)

      //redis资源连接初始化
      initRedisConn()

      //数据初始化加载到缓存
      reloadDB()


      //定时更新缓存
      //调度资源
      scheduled  = new ScheduledThreadPoolExecutor(2)
      val initialDelay: Long = 0l
      val period :Long = 60l
      scheduled.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          reloadDB()
        }
      }, initialDelay, period, TimeUnit.SECONDS)
    }


    /**
      * 异步执行
      * @param input
      * @param resultFuture
      */
    override def asyncInvoke(input: OrderDetailData, resultFuture: ResultFuture[OrderMWideData]): Unit = {
      try {
        //println(s"""MysqlAsyncFunction invoke.time=${CommonUtil.formatDate4Def(new Date())}""")
        val orderProductID :String = input.productID
        val pubID :String = input.pubID
        val defValue :String = ""

        /**
          * 产品维表相关数据
          * 使用列：product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type
          */
        val productJson :String = jedis.hget(QRealTimeConstant.MYDQL_DIM_PRODUCT, orderProductID)
        var productLevel = QRealTimeConstant.COMMON_NUMBER_ZERO_INT
        var productType = defValue
        var depCode = defValue
        var desCode = defValue
        var toursimType = defValue
        if(StringUtils.isNotEmpty(productJson)){
          val productRow : util.Map[String,Object] = JsonUtil.json2object(productJson, classOf[util.Map[String,Object]])
          productLevel = productRow.get("product_level").toString.toInt

          productType = productRow.get("product_type").toString
          depCode = productRow.get("departure_code").toString
          desCode = productRow.get("des_city_code").toString
          toursimType = productRow.get("toursim_tickets_type").toString
        }

        /**
          * 酒店维表相关数据
          */
        val pubJson :String = jedis.hget(QRealTimeConstant.MYDQL_DIM_PUB, pubID)
        var pubStar :String = defValue
        var pubGrade :String = defValue
        var isNational :String = defValue
        if(StringUtils.isNotEmpty(pubJson)){
          val pubRow : util.Map[String,Object] = JsonUtil.json2object(pubJson, classOf[util.Map[String,Object]])
          pubStar = pubRow.get("pub_star").toString
          pubGrade = pubRow.get("pub_grade").toString
          isNational = pubRow.get("is_national").toString
        }

        var orderWide = new OrderMWideData(input.orderID, input.userID, orderProductID, input.pubID,
          input.userMobile, input.userRegion, input.traffic, input.trafficGrade, input.trafficType,
          input.price, input.fee, input.hasActivity,
          input.adult, input.yonger, input.baby, input.ct,
          productLevel, productType, toursimType, depCode, desCode,
          pubStar, pubGrade, isNational)

        val orderWides = List(orderWide)
        resultFuture.complete(orderWides)
      }catch {
        //注意：加入异常处理放置阻塞产生
        case ex: Exception => {
          println(s"""ex=${ex}""")
          logger.error("DimProductMAsyncFunction.err:" + ex.getMessage)
          resultFuture.completeExceptionally(ex)
        }
      }
    }


    /**
      * 超时处理
      * @param input
      * @param resultFuture
      */
    override def timeout(input: OrderDetailData, resultFuture: ResultFuture[OrderMWideData]): Unit = {
      println(s"""DimProductMAsyncFunction timeout.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.timeout(input, resultFuture)
    }

    /**
      * 关闭
      */
    override def close(): Unit = {
      println(s"""DimProductMAsyncFunction close.time=${CommonUtil.formatDate4Def(new Date())}""")
      super.close()
      pool.close()
    }
  }
}
