package com.qf.bigdata.realtime.flink.streaming.rdo

//维度封装
object QRealTimeDimDO {

  //产品维度的封装
  case class ProductDimDO(productID:String, productLevel:Int, productType:String,
                          depCode:String, desCode:String, toursimType:String)
  //TODO  ---- 酒店暂未封装
}
