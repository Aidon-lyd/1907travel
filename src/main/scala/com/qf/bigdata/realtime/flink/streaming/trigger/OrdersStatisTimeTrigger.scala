package com.qf.bigdata.realtime.flink.streaming.trigger

import java.util.concurrent.TimeUnit

import com.qf.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.OrderDetailData
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala.createTypeInformation


class OrdersStatisTimeTrigger(maxInterval :Long, timeUnit:TimeUnit) extends Trigger[OrderDetailData, TimeWindow]{

  //订单统计业务的处理时间状态
  val TRIGGER_ORDER_STATE_TIME_DESC = "TRIGGER_ORDER_STATE_TIME_DESC"
  var ordersTimeStateDesc :ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](TRIGGER_ORDER_STATE_TIME_DESC, createTypeInformation[Long])
  var ordersTimeState :ValueState[Long] = _


  /**
   * 元素处理
   * @param element 数据类型
   * @param timestamp 元素时间
   * @param window 窗口
   * @param ctx 上下文环境
   * @return
   */
  override def onElement(element: OrderDetailData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //计数状态
    ordersTimeState = ctx.getPartitionedState(ordersTimeStateDesc)

    //处理时间间隔
    val maxIntervalTimestamp :Long = Time.of(maxInterval,timeUnit).toMilliseconds
    val curProcessTime :Long = ctx.getCurrentProcessingTime

    //当前处理时间到达或超过上次处理时间+间隔后触发本次窗口操作
    var nextProcessingTime = TimeWindow.getWindowStartWithOffset(curProcessTime, 0, maxIntervalTimestamp) + maxIntervalTimestamp
    ctx.registerProcessingTimeTimer(nextProcessingTime)

    ordersTimeState.update(nextProcessingTime)
    return TriggerResult.CONTINUE
  }

  /**
   * 一个已注册的处理时间计时器启动时调用
   * @param time
   * @param window
   * @param ctx
   * @return
   */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE;
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE;
  }

  /**
   * 执行任何需要清除的相应窗口
   * @param window
   * @param ctx
   */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //删除处理时间的定时器
    ctx.deleteProcessingTimeTimer(ordersTimeState.value())
  }

  /**
   * 合并窗口
   * @param window
   * @param ctx
   */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    val windowMaxTimestamp = window.maxTimestamp()
    if(windowMaxTimestamp > ctx.getCurrentWatermark){
      ctx.registerProcessingTimeTimer(windowMaxTimestamp)
    }
  }
}