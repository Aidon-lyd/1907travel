package com.qf.bigdata.realtime.flink.streaming.rdo

/**
 * 封装接入的数据模型
 * ---> kafka中的数据
 * --->维度数据
 * --->kv类型的数据
 * --->中间数据
 */

object QRealtimeDO {

  ////各种类的基类---顶级父类
  abstract class QBase{
    def ct:Long
  }

  //key-value数据类型
  case class QKVBase(key:String,value:String)

  //行为维度封装
  case class ActionDim(code:String,desc:String,remark:String)

  /**
   * 日志行为原数据封装
   */
  case class UserLogData(sid:String,userDevice:String,userDeviceType:String,
                         os:String,userId:String,userRegion:String,userRegionIP:String,longitude:String,
                        latitude:String,manufacturer:String,carrier:String,networkType:String,
                         duration:String,exts:String,action:String,eventType:String,
                        ct:Long)

  //用户启动日志数据
  case class UserLogLaunchData(userDevice:String,userId:String,userRegion:String,
                               userRegionIp:String,networkType:String,ct:Long)

  //用户行为浏览日志操作数据
  case class UserLogPageViewData(sid:String,userDevice:String,userDeviceType:String,
                                 os:String,userId:String,userRegion:String,userRegionIP:String,longitude:String,
                                 latitude:String,manufacturer:String,carrier:String,networkType:String,
                                 duration:String,action:String,eventType:String,
                                 ct:Long,targetId:String)


  //用户行为page停留时长 --- 告警
  case class UserLogPageViewAlertData(userDevice:String,userId:String,userRegion:String,
                                      userRegionIp:String,duration:String,ct:Long)


  //用户行为page停留时长 -- 统计页面浏览相关
  case class UserLogPageViewAggMeanData(access:Long,users:Long,totalDuration:Long,avgDuration:Long,maxTimeStamp:Long)

  /**
   * 页面浏览日志统计数据(停留时长不符合需求)
   */
  case class UserLogPageViewLowDurationAggMeanData(access:Long, users:Long, totalDuration:Long,maxTimestamp:Long)



  //用户点击数据封装
  case class UserLogClickData(sid:String,userDevice:String,userDeviceType:String,
                                 os:String,userId:String,userRegion:String,longitude:String,
                                 latitude:String,manufacturer:String,carrier:String,networkType:String,
                                 duration:String,action:String,eventType:String,
                                 ct:Long,targetId:String,eventTargetType:String)

  //用户点击行为统计---维度封装
  case class UserLogClickDimData(userRegion:String,eventTargetType:String)

  //用户点击行为统计---度量值封装
  case class UserLogClickMeanData(access:Long,users:Long)

  /**
   *用户行为日志点击行为统计输出数据
   */
  case class UserLogClickDimMeanData2(eventTargetType:String, access:Long, users:Long, startWindowTime:Long, endWindowTime:Long)

  /**
   * 用户行为日志产品列表浏览数据
   */
  case class UserLogViewListData(sid:String, userDevice:String, userDeviceType:String, os:String,
                                 userID:String,userRegion:String, userRegionIP:String, lonitude:String, latitude:String,
                                 manufacturer:String, carrier:String, networkType:String, duration:String, exts:String,
                                 action:String, eventType:String, ct:Long, hotTarget:String)


  //用户行为产品浏览数据封装
  case class UserLogViewListFactData(sid:String,userDevice:String,userDeviceType:String,
                                 os:String,userId:String,userRegion:String,longitude:String,
                                 latitude:String,manufacturer:String,carrier:String,networkType:String,
                                 duration:String,action:String,eventType:String,
                                 ct:Long,targetId:String,hotTarget:String,travelSend:String,
                                     travelSendTime:String,travelTime:String,productLevel:String,
                                     productType:String)


  //===订单业务=================================================================================
  /**
   *订单明细数据
   */
  case class OrderDetailData(orderID:String, userID:String, productID:String, pubID:String,
                             userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                             price:Int, fee:Int, hasActivity:String,
                             adult:String, yonger:String, baby:String, ct:Long) extends QBase

  case class OrderDetailSimData(orderID:String, userID:String, productID:String, pubID:String,
                                userRegion:String, traffic:String,
                                price:Int, fee:Int, hasActivity:String,ct:Long)


  /**
   * 订单会话窗口维度列集
   */
  case class OrderDetailSessionDimData(traffic:String, hourTime:String)


  case class OrderTrafficDimData(productID:String, traffic:String)

  case class OrderTrafficDimMeaData(productID:String, traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, totalFee:Long)

  case class OrderTrafficMidData(productID:String, traffic:String, orders:Long, members:Long,totalFee:Long)

  /**
   * 订单宽表会话时间窗口统计结果列集
   */
  case class OrderDetailSessionDimMeaData(traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)



  /**
   * 处理时间窗口类
   * @param start 开始时间
   * @param end 结束时间
   */
  case class QProcessWindow(var start:Long, var end:Long)



  /**
   * 订单统计维度列集
   */
  case class OrderDetailAggDimData(userRegion:String, traffic:String)


  /**
   * 订单统计累加器
   */
  case class OrderAccData(orders:Long, totalFee:Long)


  /**
   * 订单宽表时间窗口统计度量列集
   */
  case class OrderDetailTimeAggMeaData(orders:Long, maxFee:Long, totalFee:Long, members:Long)



  /**
   * 订单宽表时间窗口统计结果列集
   */
  case class OrderDetailTimeAggDimMeaData(userRegion:String, traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)


  /**
   * 订单时间窗口统计结果列集
   */
  case class OrderDetailStatisData(traffic:String, etTime:String,orders:Long, users:Long, totalFee:Long, ptTime:String)


  case class OrderDetailStatisCustomerData(traffic:String, etTime:String, ptTime:String, orders:Long, users:Long, totalFee:Long)


  case class OrderDetailTimeStatisData(userRegion:String, traffic:String, startWindowTime:Long, endWindowTime:Long,orders:Long, users:Long, totalFee:Long)


  case class OrderWindowStatisData(traffic:String, etTime:String, startWindowTime:Long, endWindowTime:Long,orders:Long, totalFee:Long)


  //---订单宽表-----------------------------------------
  /**
   *订单明细数据(宽表数据)
   */
  case class OrderWideData(orderID:String, userID:String, productID:String, pubID:String,
                           userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                           price:Int, fee:Long, hasActivity:String,
                           adult:String, yonger:String, baby:String, ct:Long,
                           productLevel:Int, productType:String, toursimType:String, depCode:String, desCode:String)


  /**
   *订单明细数据(多维表宽表数据)
   */
  case class OrderMWideData(orderID:String, userID:String, productID:String, pubID:String,
                            userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                            price:Int, fee:Long, hasActivity:String,
                            adult:String, yonger:String, baby:String, ct:Long,
                            productLevel:Int, productType:String, toursimType:String, depCode:String, desCode:String,
                            pubStar:String, pubGrade:String, isNational:String)



  /**
   * 订单宽表统计维度列集
   */
  case class OrderWideAggDimData(productType:String, toursimType:String)

  /**
   * 订单宽表计数窗口统计度量列集
   */
  case class OrderWideCountAggMeaData(orders:Long, maxFee:Long, totalFee:Long, members:Long)

  /**
   * 订单宽表计数窗口统计结果列集
   */
  case class OrderWideCountAggDimMeaData(productType:String, toursimType:String,endWindowTime:Long, orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)


  /**
   * 订单宽表时间窗口统计度量列集
   */
  case class OrderWideTimeAggMeaData(orders:Long, maxFee:Long, totalFee:Long, members:Long)



  /**
   * 订单宽表时间窗口统计结果列集
   */
  case class OrderWideTimeAggDimMeaData(productType:String, toursimType:String, startWindowTime:Long, endWindowTime:Long,orders:Long, maxFee:Long, totalFee:Long, members:Long, avgFee:Double)


  /**
   * 订单宽表统计结果列集合
   */
  case class OrderWideCustomerStatisData(productType:String, toursimType:String, startWindowTime:Long, endWindowTime:Long,orders:Long, users:Long, totalFee:Long)
  //===============================================================================
}
