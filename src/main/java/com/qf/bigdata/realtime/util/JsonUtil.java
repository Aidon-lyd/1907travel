package com.qf.bigdata.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.qf.bigdata.realtime.util.CommonUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

/**
 * json的工具类
 */
public class JsonUtil implements Serializable{

    public static final String DEF_DATEFORMAT_ALL = "yyyyMMddHHmmss";

    public static final String DEF_DATEFORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String DEF_ES_DATEFORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    public static final String DEF_ES_DATEFORMATZ = "yyyy-MM-dd'T'HH:mm:ss'Z'";


    public static Gson gson = null;
    static {
        GsonBuilder gb = new GsonBuilder().serializeNulls().setDateFormat("yyyy-MM-dd HH:mm:ss");
        gb.setLongSerializationPolicy(LongSerializationPolicy.STRING);
        gson = gb.create();
    }


    /**
     * json -> list
     * @param json
     * @return
     */
    public static List<String> gJson2List(String json) {
        List<String> result = new ArrayList<String>();
        JSONArray jarray = JSON.parseArray(json);
        if(!jarray.isEmpty()){
            result = jarray.toJavaList(String.class);
        }
        return result;
    }

    /**
     * obj -> json
     * @param obj
     * @return
     */
    public static String gObject2Json(Object obj) {
        String json = gson.toJson(obj, obj.getClass());
        return json;
    }



    /**
     * obj -> json
     * @param obj
     * @return
     */
    public static Map<String,Object> gObject2Map(Object obj) {
        String json = gson.toJson(obj, obj.getClass());
        Map<String,Object> result = gson.fromJson(json,Map.class);
        return result;
    }

    /**
     * json -> map
     * @param str
     * @return
     */
    public static Map<String,Object> gJson2Map(String str) {
        Map<String,Object> result = gson.fromJson(str,Map.class);
        return result;
    }


    /**
     * obj -> json
     * @param obj
     * @return
     */
    public static String object2jsonConfig(Object obj) {
        //JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd"; 全局日期格式化
        SerializeConfig config = new SerializeConfig();
        return JSON.toJSONString(obj, config);
    }

    public static String object2json(Object obj) {
        //JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd"; 全局日期格式化
        return JSON.toJSONString(obj);
    }

    public static String object2json(Object obj, String dataFormat) {
        if(StringUtils.isEmpty(dataFormat)){
            dataFormat = "yyyy-MM-dd HH:mm:ss";
        }
        return JSON.toJSONStringWithDateFormat(obj, dataFormat, SerializerFeature.WriteDateUseDateFormat);
    }

    public static String object2json4DefDateFormat(Object obj) {
        return JSON.toJSONStringWithDateFormat(obj, DEF_DATEFORMAT_ALL, SerializerFeature.WriteDateUseDateFormat);
    }

    public static String object2json4DefDateFormatTZ(Object obj) {
        return JSON.toJSONStringWithDateFormat(obj, DEF_ES_DATEFORMATZ, SerializerFeature.WriteDateUseDateFormat);
    }

    public static String object2json4ISO(Object obj) {
        return JSON.toJSONStringWithDateFormat(obj, DEF_ES_DATEFORMAT, SerializerFeature.WriteDateUseDateFormat);
    }

    public static <T> T json2obj(Class<T> obj, String dataFormat, String json) {
        if(StringUtils.isEmpty(dataFormat)){
            dataFormat = "yyyy-MM-dd HH:mm:ss";
            JSON.DEFFAULT_DATE_FORMAT = dataFormat;
        }

        if(StringUtils.isEmpty(json)){
            return null;
        }

        return JSON.parseObject(json, obj);
    }


    public static <T> T json2object(String json,Class<T> cls) {
        JSON.DEFFAULT_DATE_FORMAT = DEF_DATEFORMAT;
        return JSON.parseObject(json, cls);
    }




    public static JSONObject json2object(String json) {
        JSON.DEFFAULT_DATE_FORMAT = DEF_DATEFORMAT;
        return JSON.parseObject(json);
    }


    //===================================================================================


    public static Map<String,String> json2Map4Array(String pKey, String json,boolean addParent) {
        Map<String,String> result = new HashMap<String,String>();
        JSON.DEFFAULT_DATE_FORMAT = DEF_DATEFORMAT;

        JSONArray jArray = JSON.parseArray(json);

        int idx = 0;
        for(Iterator<Object> ite = jArray.iterator(); ite.hasNext();){
            Object value = ite.next();

            if (value instanceof JSONObject) {

                JSONObject jObj = (JSONObject) value;

                String childJson = jObj.toJSONString();

                String lastKey = String.valueOf(idx);
                if(addParent){
                    lastKey = pKey+"_"+idx;
                }

                Map<String,String> childMap = json2object4MapCascade(lastKey, childJson, addParent);

                result.putAll(childMap);

            }else {

                String key = pKey+"_"+idx;
                result.put(key, value.toString());

            }

            idx++;

        }
        return  result;
    }


    public static Map<String,String> json2object4MapCascade(String pKey, String json, boolean addParent) {

        Map<String,String> result = new HashMap<String,String>();

        JSON.DEFFAULT_DATE_FORMAT = DEF_DATEFORMAT;
        JSONObject root = JSON.parseObject(json);

        Set<Map.Entry<String, Object>> childs = root.entrySet();
        if(null != childs){
            for(Iterator<Map.Entry<String, Object>> ite = childs.iterator(); ite.hasNext();){
                Map.Entry<String, Object> entry = ite.next();
                String key = entry.getKey();
                Object value = entry.getValue();

                //System.out.println("key=" + key +  ",value=" + value.getClass().getName());
                if (value instanceof JSONObject) {

                    //结构类型(如引用类,map)
                    JSONObject jObj = (JSONObject) value;

                    String childJson = jObj.toJSONString();

                    Map<String,String> childMap = json2object4MapCascade(key, childJson,addParent);

                    result.putAll(childMap);

                }else if(value instanceof JSONArray){
                    //数组类型
                    JSONArray jArray = (JSONArray)value;

                    String childJson = jArray.toJSONString();

                    Map<String,String> childMap = json2Map4Array(key, childJson, addParent);

                    result.putAll(childMap);

                }else{
                    //普通类型
                    String lastKey = key + pKey;
                    if(addParent){
                        if(!StringUtils.isEmpty(pKey)){
                            lastKey = pKey+"_"+key;
                        }
                    }

                    result.put(lastKey, value.toString());
                }
            }
        }

        return result;
    }


    public static Map<String,String> json2object4MapSingle(String json) {

        Map<String,String> result = new HashMap<String,String>();

        JSON.DEFFAULT_DATE_FORMAT = DEF_DATEFORMAT;
        JSONObject root = JSON.parseObject(json);

        Set<Map.Entry<String, Object>> childs = root.entrySet();
        if(null != childs){
            for(Iterator<Map.Entry<String, Object>> ite = childs.iterator(); ite.hasNext();){
                Map.Entry<String, Object> entry = ite.next();
                String key = entry.getKey();
                Object value = entry.getValue();

                result.put(key, value.toString());
            }
        }

        return result;
    }

    public static void main(String[] args){


        String json = "[\"P40\",\"P46\",\"P28\",\"P62\"]";
        List<String> list = gJson2List(json);
        System.out.println(list);

        String str = "{\"targetID\":\"P25\"}";
        Map<String,Object> datas = gJson2Map(str);
        System.out.println(datas.toString());

        //String json = CommonUtil.file2String("schema/p1.json");
        //Map data = JsonUtil.json2object4MapSingle(json);

        //Map data = JsonUtil.json2object4MapCascade("",json,false);

        //Map data2 = JsonUtil.json2object(json, Map.class);
        //System.out.println(data);

        //System.out.println("ok");

    }

}
