package com.qf.bigdata.realtime.util;


import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 读取指定目录或者指定的properties文件
 */
public class PropertyUtil implements Serializable{

    private static Logger log = LoggerFactory.getLogger(PropertyUtil.class);

    public static final String PROPERTY_FILTER = ".properties";

    /**
     * 加载所有属性文件
     * @param path
     * @return
     */
    public static Map<String,Properties> loadAllProperties(String path){
        Map<String,Properties> result = new HashMap<String,Properties>();
        Reader reader = null;
        try{
            File propertiesDir = new File(path);
            FilenameFilter nameFilter = new FileNameFilterImpl(PROPERTY_FILTER);
            if(propertiesDir.exists() && propertiesDir.isDirectory()){
                File[] propertiesFile = propertiesDir.listFiles(nameFilter);
                if(null != propertiesFile && propertiesFile.length >0){
                    for(File propertyFile :propertiesFile){
                        String name = propertyFile.getName();

                        reader = new FileReader(propertyFile);
                        Properties properties = new Properties();
                        properties.load(reader);

                        result.put(name,properties);
                        reader.close();
                    }
                }
            }
        }catch(IOException ioe){
            log.error("load all Properties:" + ioe.getMessage());
        } finally {
            try{
                if(null != reader){
                    reader.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 加载指定的属性文件
     * @param path
     * @return
     */
    public static Properties loadProperties(String path){
        Properties properties = null;
        Reader reader = null;
        try{
            File proFile = new File(path);
            FilenameFilter nameFilter = new FileNameFilterImpl(PROPERTY_FILTER);
            if(proFile.exists() && proFile.isFile()){
                reader = new FileReader(proFile);
                properties = new Properties();
                properties.load(reader);
            }
        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != reader){
                    reader.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return properties;
    }


    /**
     * 读取资源文件
     * @param proPath
     * @return
     */
    public static Properties readProperties(String proPath){
        Validate.notEmpty(proPath, "properties is empty");

        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);
        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return properties;
    }


    /**
     * 读取资源文件
     * @param proPath
     * @return
     */
    public static Map<String,Object> readProperties2Map(String proPath){
        Validate.notEmpty(proPath, "properties is empty");

        Map<String,Object> result = new HashMap<String,Object>();

        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);

            Set<Map.Entry<Object,Object>> entrySet = properties.entrySet();
            for(Map.Entry<Object,Object> entry : entrySet){
                String key = entry.getKey().toString();
                Object value = entry.getValue();

                result.put(key, value);
            }

        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return result;
    }


    public static Map<String,String> readProperties2Map4String(String proPath){
        Validate.notEmpty(proPath, "properties is empty");

        Map<String,String> result = new HashMap<String,String>();

        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);

            Set<Map.Entry<Object,Object>> entrySet = properties.entrySet();
            for(Map.Entry<Object,Object> entry : entrySet){
                String key = entry.getKey().toString();
                String value = entry.getValue().toString();

                result.put(key, value);
            }

        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 获取指定属性文件的值
     * @param proFile
     * @param key
     * @return
     */
    public static String getProperty(String proFile, String key) {
        String result = null;
        try{
            if(!StringUtils.isEmpty(proFile)){
                Properties props = loadProperties(proFile);
                if(null != props){
                    result =  props.getProperty(key).trim();
                }
            }
        }catch(Exception e){
            log.error("PropertiesUtil.getProperty:" + e.getMessage());
        }
        return result;
    }

    public static Integer getProperty4Int(Properties props, String key) {
        Integer result = null;
        try{
            result =  Integer.parseInt(props.getProperty(key).trim());
        }catch(Exception e){
            log.error("PropertiesUtil.getProperty4Int:" + e.getMessage());
        }
        return result;
    }



    public static boolean getProperty(Properties props, String key) {
        Boolean result = null;
        try{
            result =  Boolean.parseBoolean(props.getProperty(key).trim());
        }catch(Exception e){
            log.error("PropertiesUtil.getProperty4Int:" + e.getMessage());
        }
        return result;
    }


    public static void main(String[] args) {
        System.out.println(PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL()).getProperty("bootstrap.servers"));
    }
}
