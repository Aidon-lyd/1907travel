package com.qf.bigdata.realtime.flink.streaming.rdo.typeinfomation

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

//维度数据类型封装
object QRealTimeDimTypeInfomation {
  //获取维度数据类型方法
  def getProductDimFieldTypeInfos() = {
    var colTypeInfos = List[TypeInformation[_]]()
    //向colTypeInfos中加入字段类型
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.INT_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos
  }
}
