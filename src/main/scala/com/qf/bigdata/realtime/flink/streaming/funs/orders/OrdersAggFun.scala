package com.qf.bigdata.realtime.flink.streaming.funs.orders

import java.util.{Comparator, Date}
import java.util.concurrent.TimeUnit
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant
import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{OrderAccData, OrderDetailAggDimData, OrderDetailData, OrderDetailSessionDimData, OrderDetailSessionDimMeaData, OrderDetailStatisData, OrderDetailTimeAggDimMeaData, OrderDetailTimeAggMeaData, OrderTrafficDimData, OrderTrafficDimMeaData, OrderWideAggDimData, OrderWideCountAggDimMeaData, OrderWideCountAggMeaData, OrderWideCustomerStatisData, OrderWideData, OrderWideTimeAggDimMeaData, OrderWideTimeAggMeaData, QProcessWindow}
import com.qf.bigdata.realtime.util.CommonUtil
import org.apache.flink.api.common.functions.{AggregateFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
* 旅游产品聚合类操作对应函数
*/
object OrdersAggFun {

  //===订单窗口统计====================================================

  /**
    * 订单聚合数据的窗口处理函数
    */
  class OrderTrafficTimeWindowFun extends WindowFunction[OrderDetailTimeAggMeaData, OrderTrafficDimMeaData, OrderTrafficDimData, TimeWindow]{
    override def apply(key: OrderTrafficDimData, window: TimeWindow, input: Iterable[OrderDetailTimeAggMeaData], out: Collector[OrderTrafficDimMeaData]): Unit = {
      //分组维度
      val productID = key.productID
      val traffic = key.traffic

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData: OrderDetailTimeAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }

      //窗口时间
      val startWindowTime = window.getStart
      val endWindowTime = window.getEnd

      val owDMData = OrderTrafficDimMeaData(productID, traffic, startWindowTime, endWindowTime, outOrders, outFees)
      out.collect(owDMData)
    }
  }



  /**
    * 订单明细数据基于时间的窗口处理函数(预聚合操作)
    */
  class OrderDetailTimeAggFun extends AggregateFunction[OrderDetailData, OrderDetailTimeAggMeaData, OrderDetailTimeAggMeaData] {
    /**
      * 创建累加器，，并初始化
      */
    override def createAccumulator(): OrderDetailTimeAggMeaData = {
      OrderDetailTimeAggMeaData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO,
        QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
    }

    /**
      * 累加开始
      */
    override def add(value: OrderDetailData, accumulator: OrderDetailTimeAggMeaData): OrderDetailTimeAggMeaData = {
      val orders = QRealTimeConstant.COMMON_NUMBER_ONE + accumulator.orders
      var maxFee = accumulator.maxFee.max(value.fee)
      val totalFee = value.fee + accumulator.totalFee
      val orderMembers = value.adult.toInt + value.yonger.toInt + value.baby.toInt
      val members = orderMembers + accumulator.members

      OrderDetailTimeAggMeaData(orders, maxFee, totalFee, members)
    }

    /**
      * 获取结果数据
      */
    override def getResult(accumulator: OrderDetailTimeAggMeaData): OrderDetailTimeAggMeaData = {
      accumulator
    }

    /**
      * 合并中间数据
      */
    override def merge(a: OrderDetailTimeAggMeaData, b: OrderDetailTimeAggMeaData): OrderDetailTimeAggMeaData = {
      val orders = a.orders + a.orders
      var maxFee = a.maxFee.max(b.maxFee)
      val totalFee = a.totalFee + b.totalFee
      val members = a.members + b.members
      OrderDetailTimeAggMeaData(orders, maxFee, totalFee, members)
    }
  }

  /**
    * 订单明细数据的窗口处理函数
    */
  class OrderDetailTimeWindowFun extends WindowFunction[OrderDetailTimeAggMeaData, OrderDetailTimeAggDimMeaData, OrderDetailAggDimData, TimeWindow]{
    override def apply(key: OrderDetailAggDimData, window: TimeWindow, input: Iterable[OrderDetailTimeAggMeaData], out: Collector[OrderDetailTimeAggDimMeaData]): Unit = {
      //分组维度
      val userRegion = key.userRegion
      val traffic = key.traffic

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      //将窗口的输入数据进行循环计算
      for(meaData: OrderDetailTimeAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val startWindowTime = window.getStart
      val endWindowTime = window.getEnd

      val owDMData = OrderDetailTimeAggDimMeaData(userRegion, traffic, startWindowTime, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      //输出
      out.collect(owDMData)
    }
  }





  /**
    * 订单统计数据的窗口处理函数
    */
  class OrderStatisWindowProcessFun extends ProcessWindowFunction[OrderDetailData, OrderDetailStatisData, OrderDetailSessionDimData, TimeWindow]{

    //参与订单用户数量的状态描述名称
    val ORDER_STATE_USER_DESC = "ORDER_STATE_USER_DESC"
    var usersState :ValueState[mutable.Set[String]] = _
    var usersStateDesc :ValueStateDescriptor[mutable.Set[String]] = _

    //订单数量的状态描述名称
    val ORDER_STATE_ORDERS_DESC = "ORDER_STATE_ORDERS_DESC"
    var ordersAccState :ValueState[OrderAccData] = _
    var ordersAccStateDesc :ValueStateDescriptor[OrderAccData] = _


    /**
      * 相关连接资源初始化(如果需要)
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      //订单用户列表的状态数据
      usersStateDesc = new ValueStateDescriptor[mutable.Set[String]](ORDER_STATE_USER_DESC, createTypeInformation[mutable.Set[String]])
      usersState = this.getRuntimeContext.getState(usersStateDesc)

      //订单状态数据：订单度量(订单总数量、订单总费用)
      ordersAccStateDesc = new ValueStateDescriptor[OrderAccData](ORDER_STATE_ORDERS_DESC, createTypeInformation[OrderAccData])
      ordersAccState = this.getRuntimeContext.getState(ordersAccStateDesc)
    }




    /**
      * 数据处理
      * @param out
      */
    override def process(key: OrderDetailSessionDimData, context: Context, elements: Iterable[OrderDetailData], out: Collector[OrderDetailStatisData]): Unit = {
      //订单分组维度：出行方式、事件时间
      val traffic = key.traffic
      val hourTime = key.hourTime

      //时间相关
      val curWatermark :Long = context.currentWatermark
      val ptTime:String = CommonUtil.formatDate4Timestamp(context.currentProcessingTime, QRealTimeConstant.FORMATTER_YYYYMMDDHHMMSS)


      //状态相关数据
      var userKeys :mutable.Set[String] = usersState.value
      var ordersAcc :OrderAccData =  ordersAccState.value
      if(null == ordersAcc){
        ordersAcc = new OrderAccData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
      }
      if(null == userKeys){
        userKeys = mutable.Set[String]()
      }

      //数据聚合处理
      var totalOrders :Long = ordersAcc.orders
      var totalFee :Long = ordersAcc.totalFee
      var usersCount :Long = userKeys.size
      for(element <- elements){
        //度量数据处理
        val userID = element.userID
        totalOrders +=  1
        totalFee += element.fee

        //UV判断
        if(!userKeys.contains(userID)){
          userKeys += userID
        }
        usersCount = userKeys.size
      }

      val orderDetailStatis = new OrderDetailStatisData(traffic, hourTime, totalOrders, usersCount, totalFee, ptTime)
      //println(s"""orderDetailStatis=${orderDetailStatis}""")
      out.collect(orderDetailStatis)

      //状态数据更新
      usersState.update(userKeys)
      ordersAccState.update(new OrderAccData(totalOrders, totalFee))
    }


    /**
      * 状态数据清理
      * @param context
      */
    override def clear(context: Context): Unit = {
      usersState.clear()
      ordersAccState.clear()
    }


  }


  /**
    * 订单业务：自定义分组处理函数
    */
  class OrderCustomerStatisKeyedProcessFun(maxCount :Long, maxInterval :Long, timeUnit:TimeUnit) extends KeyedProcessFunction[OrderWideAggDimData, OrderWideData, OrderWideCustomerStatisData] {

    //参与订单的用户数量状态描述名称
    val CUSTOMER_ORDER_STATE_USER_DESC = "CUSTOMER_ORDER_STATE_USER_DESC"
    var customerUserState :ValueState[mutable.Set[String]] = _
    var customerUserStateDesc :ValueStateDescriptor[mutable.Set[String]] = _

    //订单数量状态描述名称
    val CUSTOMER_ORDER_STATE_ORDERS_DESC = "CUSTOMER_ORDER_STATE_ORDERS_DESC"
    var customerOrdersAccState :ValueState[OrderAccData] = _
    var customerOrdersAccStateDesc :ValueStateDescriptor[OrderAccData] = _

    //统计时间范围的状态描述名称
    val CUSTOMER_ORDER_STATE_PROCESS_DESC = "CUSTOMER_ORDER_STATE_PROCESS_DESC"
    var customerProcessState :ValueState[QProcessWindow] = _
    var customerProcessStateDesc :ValueStateDescriptor[QProcessWindow] = _


    /**
      * 初始化
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {

      //状态数据：订单UV
      customerUserStateDesc = new ValueStateDescriptor[mutable.Set[String]](CUSTOMER_ORDER_STATE_USER_DESC, createTypeInformation[mutable.Set[String]])
      customerUserState = this.getRuntimeContext.getState(customerUserStateDesc)

      //状态数据：订单度量(订单总数量、订单总费用)
      customerOrdersAccStateDesc = new ValueStateDescriptor[OrderAccData](CUSTOMER_ORDER_STATE_ORDERS_DESC, createTypeInformation[OrderAccData])
      customerOrdersAccState = this.getRuntimeContext.getState(customerOrdersAccStateDesc)

      //处理时间
      customerProcessStateDesc = new ValueStateDescriptor[QProcessWindow](CUSTOMER_ORDER_STATE_PROCESS_DESC, createTypeInformation[QProcessWindow])
      customerProcessState = this.getRuntimeContext.getState(customerProcessStateDesc)
    }

    /**
      * 处理数据
      * @param value 元素数据
      * @param ctx 上下文环境对象
      * @param out 输出结果
      */
    override def processElement(value: OrderWideData, ctx: KeyedProcessFunction[OrderWideAggDimData, OrderWideData, OrderWideCustomerStatisData]#Context, out: Collector[OrderWideCustomerStatisData]): Unit = {
      //原始数据
      val productType = value.productType
      val toursimType = value.toursimType
      val userID = value.userID
      val fee = value.fee

      //记录时间
      val curProcessTime = ctx.timerService().currentProcessingTime()
      val maxIntervalTimestamp :Long = Time.of(maxInterval,TimeUnit.MINUTES).toMilliseconds
      var nextProcessingTime = TimeWindow.getWindowStartWithOffset(curProcessTime, 0, maxIntervalTimestamp) + maxIntervalTimestamp

      //时间触发条件：当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
      if(customerProcessState.value() == null){
        customerProcessState.update(new QProcessWindow(curProcessTime, curProcessTime))
        ctx.timerService().registerProcessingTimeTimer(nextProcessingTime)
      }
      val qProcessWindow :QProcessWindow = customerProcessState.value()
      if(curProcessTime >= qProcessWindow.end){
        qProcessWindow.start = qProcessWindow.end
        qProcessWindow.end = curProcessTime
      }
      customerProcessState.update(qProcessWindow)
      val startWindowTime = qProcessWindow.start
      val endWindowTime = qProcessWindow.end

      //PV等度量统计
      var ordersAcc :OrderAccData =  customerOrdersAccState.value
      if(null == ordersAcc){
        ordersAcc = new OrderAccData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
      }
      var totalOrders :Long = ordersAcc.orders + 1
      var totalFee :Long = ordersAcc.totalFee + fee
      customerOrdersAccState.update(new OrderAccData(totalOrders, totalFee))

      //UV判断
      var userKeys :mutable.Set[String] = customerUserState.value
      if(null == userKeys){
        userKeys = mutable.Set[String]()
      }
      if(!userKeys.contains(userID)){
        userKeys += userID
      }
      val users = userKeys.size
      customerUserState.update(userKeys)

      //数量触发条件：maxCount
      if(totalOrders >= maxCount){
        val orderDetailStatisData = OrderWideCustomerStatisData(productType, toursimType, startWindowTime, endWindowTime,totalOrders, users, totalFee)

        out.collect(orderDetailStatisData)
        customerOrdersAccState.clear()
        customerUserState.clear()
      }
    }


    /**
      * 定时器触发
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[OrderWideAggDimData, OrderWideData, OrderWideCustomerStatisData]#OnTimerContext, out: Collector[OrderWideCustomerStatisData]): Unit = {
      val key :OrderWideAggDimData = ctx.getCurrentKey
      val productType = key.productType
      val toursimType = key.toursimType

      //时间触发条件：当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
      if(customerProcessState.value() == null){
        customerProcessState.update(new QProcessWindow(timestamp, timestamp))
      }
      val maxIntervalTimestamp :Long = Time.of(maxInterval,timeUnit).toMilliseconds
      var nextProcessingTime = TimeWindow.getWindowStartWithOffset(timestamp, 0, maxIntervalTimestamp) + maxIntervalTimestamp
      ctx.timerService().registerProcessingTimeTimer(nextProcessingTime)
      val startWindowTime = customerProcessState.value().start

      //度量数据
      var ordersAcc :OrderAccData =  customerOrdersAccState.value
      if(null == ordersAcc){
        ordersAcc = new OrderAccData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
      }
      var totalOrders :Long = ordersAcc.orders + 1
      var totalFee :Long = ordersAcc.totalFee + QRealTimeConstant.COMMON_NUMBER_ZERO

      //UV判断
      var userKeys :mutable.Set[String] = customerUserState.value
      if(null == userKeys){
        userKeys = mutable.Set[String]()
      }
      val users = userKeys.size

      //触发
      val orderDetailStatisData = OrderWideCustomerStatisData(productType, toursimType, startWindowTime, timestamp,totalOrders, users, totalFee)
      out.collect(orderDetailStatisData)

      customerOrdersAccState.clear()
      customerUserState.clear()
    }
  }



  //===订单宽表====================================================

  /**
    * 订单宽表数据的窗口预聚合函数
    */
  class OrderWideTimeAggFun extends AggregateFunction[OrderWideData, OrderWideTimeAggMeaData, OrderWideTimeAggMeaData] {
    /**
      * 创建累加器
      */
    override def createAccumulator(): OrderWideTimeAggMeaData = {
      OrderWideTimeAggMeaData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO,
        QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
    }

    /**
      * 累加开始
      */
    override def add(value: OrderWideData, accumulator: OrderWideTimeAggMeaData): OrderWideTimeAggMeaData = {
      val orders = QRealTimeConstant.COMMON_NUMBER_ONE + accumulator.orders
      var maxFee = accumulator.maxFee.max(value.fee)
      val totalFee = value.fee + accumulator.totalFee
      val orderMembers = value.adult.toInt + value.yonger.toInt + value.baby.toInt
      val members = orderMembers + accumulator.members
      OrderWideTimeAggMeaData(orders, maxFee, totalFee, members)
    }

    /**
      * 获取结果数据
      */
    override def getResult(accumulator: OrderWideTimeAggMeaData): OrderWideTimeAggMeaData = {
      accumulator
    }

    /**
      * 合并中间数据
      */
    override def merge(a: OrderWideTimeAggMeaData, b: OrderWideTimeAggMeaData): OrderWideTimeAggMeaData = {
      val orders = a.orders + a.orders
      var maxFee = a.maxFee.max(b.maxFee)
      val totalFee = a.totalFee + b.totalFee
      val members = a.members + b.members
      OrderWideTimeAggMeaData(orders, maxFee, totalFee, members)
    }
  }

  /**
    * 订单宽表数据的窗口处理函数
    */
  class OrderWideTimeWindowFun extends WindowFunction[OrderWideTimeAggMeaData, OrderWideTimeAggDimMeaData, OrderWideAggDimData, TimeWindow]{
    override def apply(key: OrderWideAggDimData, window: TimeWindow, input: Iterable[OrderWideTimeAggMeaData], out: Collector[OrderWideTimeAggDimMeaData]): Unit = {
      //分组维度
      val productType = key.productType
      val toursimType = key.toursimType

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData :OrderWideTimeAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val startWindowTime = window.getStart
      val endWindowTime = window.maxTimestamp()

      val owDMData = OrderWideTimeAggDimMeaData(productType, toursimType, startWindowTime, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      println(s"""$owDMData""")
      out.collect(owDMData)
    }
  }


  //===计数====================================================

  /**
    * 订单宽表数据的窗口处理函数
    */
  class OrderCountAggFun extends AggregateFunction[OrderDetailData, OrderWideCountAggMeaData, OrderWideCountAggMeaData] {
    /**
      * 创建累加器
      */
    override def createAccumulator(): OrderWideCountAggMeaData = {
      OrderWideCountAggMeaData(QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO,
        QRealTimeConstant.COMMON_NUMBER_ZERO, QRealTimeConstant.COMMON_NUMBER_ZERO)
    }

    /**
      * 累加开始
      */
    override def add(value: OrderDetailData, accumulator: OrderWideCountAggMeaData): OrderWideCountAggMeaData = {
      val orders = QRealTimeConstant.COMMON_NUMBER_ONE + accumulator.orders
      var maxFee = accumulator.maxFee.max(value.fee)
      val totalFee = value.fee + accumulator.totalFee
      val orderMembers = value.adult.toInt + value.yonger.toInt + value.baby.toInt
      val members = orderMembers + accumulator.members
      OrderWideCountAggMeaData(orders, maxFee, totalFee, members)
    }

    /**
      * 获取结果数据
      */
    override def getResult(accumulator: OrderWideCountAggMeaData): OrderWideCountAggMeaData = {
      accumulator
    }

    /**
      * 合并中间数据
      */
    override def merge(a: OrderWideCountAggMeaData, b: OrderWideCountAggMeaData): OrderWideCountAggMeaData = {
      val orders = a.orders + a.orders
      var maxFee = a.maxFee.max(b.maxFee)
      val totalFee = a.totalFee + b.totalFee
      val members = a.members + b.members
      OrderWideCountAggMeaData(orders, maxFee, totalFee, members)
    }
  }

  /**
    * 订单宽表数据的窗口处理函数
    */
  class OrderAggWindowFun extends WindowFunction[OrderWideCountAggMeaData, OrderDetailTimeAggDimMeaData, OrderDetailAggDimData, GlobalWindow]{
    override def apply(key: OrderDetailAggDimData, window: GlobalWindow, input: Iterable[OrderWideCountAggMeaData], out: Collector[OrderDetailTimeAggDimMeaData]): Unit = {
      //分组维度
      val userRegion = key.userRegion
      val traffic = key.traffic

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData:OrderWideCountAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val startWindowTime = 0L
      val endWindowTime = window.maxTimestamp()

      val owDMData = OrderDetailTimeAggDimMeaData(userRegion, traffic, startWindowTime, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      out.collect(owDMData)
    }
  }

  /**
    * 订单宽表数据的窗口处理函数
    */
  class OrderWideAggWindowFun extends WindowFunction[OrderWideCountAggMeaData, OrderWideCountAggDimMeaData, OrderWideAggDimData, GlobalWindow]{
    override def apply(key: OrderWideAggDimData, window: GlobalWindow, input: Iterable[OrderWideCountAggMeaData], out: Collector[OrderWideCountAggDimMeaData]): Unit = {
      //分组维度
      val productType = key.productType
      val toursimType = key.toursimType

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData:OrderWideCountAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val endWindowTime = window.maxTimestamp()

      val owDMData = OrderWideCountAggDimMeaData(productType, toursimType, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      out.collect(owDMData)
    }
  }


  //===订单计时(会话窗口)====================================================

  /**
    * 订单明细数据的窗口处理函数
    */
  class OrderDetailSessionTimeWindowFun extends WindowFunction[OrderDetailTimeAggMeaData, OrderDetailSessionDimMeaData, OrderDetailSessionDimData, TimeWindow]{
    override def apply(key: OrderDetailSessionDimData, window: TimeWindow, input: Iterable[OrderDetailTimeAggMeaData], out: Collector[OrderDetailSessionDimMeaData]): Unit = {
      //分组维度
      val traffic = key.traffic

      //度量计算
      var outOrders = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMaxFee = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outFees = QRealTimeConstant.COMMON_NUMBER_ZERO
      var outMembers = QRealTimeConstant.COMMON_NUMBER_ZERO
      for(meaData: OrderDetailTimeAggMeaData <- input){
        outOrders = outOrders + meaData.orders
        outMaxFee = outMaxFee.max(meaData.maxFee)
        outFees = outFees + meaData.totalFee
        outMembers = outMembers + meaData.members
      }
      val outAvgFee = outFees / outOrders

      //窗口时间
      val startWindowTime = window.getStart
      val endWindowTime = window.maxTimestamp()

      val owDMData = OrderDetailSessionDimMeaData(traffic, startWindowTime, endWindowTime,outOrders, outMaxFee, outFees, outMembers, outAvgFee)
      out.collect(owDMData)
    }
  }


  //===TopN产品订单统计排序=========================================================
  /**
    * 订单统计排名的窗口处理函数
    */
  class OrderTopNKeyedProcessFun(topN:Long) extends ProcessWindowFunction[OrderTrafficDimMeaData, OrderTrafficDimMeaData, OrderTrafficDimData, TimeWindow] {


  /**
    * 处理数据
    */
    override def process(key: OrderTrafficDimData, context: Context, elements: Iterable[OrderTrafficDimMeaData], out: Collector[OrderTrafficDimMeaData]): Unit = {
      //原始数据
      val productID = key.productID
      val traffic = key.traffic

      //排序比较(基于TreeSet)
      val topNContainer = new java.util.TreeSet[OrderTrafficDimMeaData](new OrderTopnComparator())

      for(element :OrderTrafficDimMeaData <- elements){
         val orders = element.orders
         val totalFee = element.totalFee
         val startWindowTime = element.startWindowTime
         val endWindowTime = element.endWindowTime

         val value = new OrderTrafficDimMeaData(productID, traffic, startWindowTime, endWindowTime,orders, totalFee)
         if(!topNContainer.isEmpty){
           // 容器大小大于 > topN,,就要移除后面的数据(较小数据)
           if(topNContainer.size() >= topN){
             val first : OrderTrafficDimMeaData = topNContainer.first()
             val result = topNContainer.comparator().compare(first, value)
             if(result < 0){
               topNContainer.pollFirst()
               topNContainer.add(value)
             }
           }else{
             topNContainer.add(value)
           }
         }else{
           topNContainer.add(value)
         }
       }

       for(data <- topNContainer){
         out.collect(data)
       }
    }
  }

  /**
    * 订单排序比较器
    */
  class OrderTopnComparator extends Comparator[OrderTrafficDimMeaData]{

    override def compare(o1: OrderTrafficDimMeaData, o2: OrderTrafficDimMeaData): Int = {
      val orders1 = o1.orders
      val orders2 = o2.orders
      val ordersComp = orders1 - orders2

      val totalFee1 = o1.totalFee
      val totalFee2 = o2.totalFee
      val totalFeeComp = totalFee1 - totalFee2

      var result = ordersComp
      if(ordersComp == 0){
        result = totalFeeComp
      }
      result.toInt
    }
  }
}
