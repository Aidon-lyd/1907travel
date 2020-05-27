package com.qf.bigdata.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.qf.bigdata.realtime.flink.constant.QRealTimeConstant;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * alibaba 数据库连接池
 */
public class DBDruid implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DBDruid.class);

    //数据源
    public DruidDataSource dataSource;

    private String driver;
    private String url;
    private String user;
    private String pass;

    public DBDruid(String driver, String url, String user, String pass){
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
    }


    //初始大小
    public static final Integer iniSize = 5;
    public static final Integer maxActive = 20;
    public static final Integer minIdle = 5;
    public static final Integer maxWait = 5 * 10000;
    public static final Integer abandonedTimeout = 600;

    public DruidDataSource getDruidDataSource() throws Exception{
        if(null == dataSource){
            dataSource = new DruidDataSource();
            //四个基本属性
            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(pass);

            //连接池属性
            dataSource.setInitialSize(iniSize);
            dataSource.setMaxActive(maxActive);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxWait(maxWait);

            //超时限制是否回收
            dataSource.setRemoveAbandoned(true);
            dataSource.setRemoveAbandonedTimeout(abandonedTimeout);
        }
        return dataSource;
    }

    /**
     * 打开连接
     * @return
     */
    public Connection getConnection(){
        Connection conn = null;
        try{
            DruidDataSource dataSource = getDruidDataSource();
            if(null != dataSource){
                conn = dataSource.getConnection();
            }
        }catch (Exception e){
            e.printStackTrace();
            logger.error("DBDruid error :", e);
        }
        return conn;
    }

    /**
     * 关闭连接池
     * @return
     */
    public void close(){
        try{
            if(null != dataSource){
                //dataSource.close();
            }
        }catch (Exception e){
            e.printStackTrace();
            logger.error("DBDruid error :", e);
        }
    }


    /**
     * sql执行结果
     * @param sql
     * @throws SQLException
     */
    public List<Row> readRecords(String sql, String schema) throws SQLException {
        Connection conn = getConnection();
        PreparedStatement ppst = null;
        ResultSet rs = null;
        List<Row> result = new ArrayList<Row>();
        if(null != conn){
            String exeSchema = schema.replaceAll("\\s*", "");
            ppst = conn.prepareStatement(sql);
            rs = ppst.executeQuery();
            List<String> resultJsons = new ArrayList<String>();
            while(rs.next()){
                List<Object> rowValues = new ArrayList<Object>();
                String[] fieldNames = exeSchema.split(QRealTimeConstant.COMMA());

                for(String fieldName : fieldNames){
                    Object fValue = rs.getObject(fieldName);
                    rowValues.add(fValue);
                }

                Row row = Row.of(rowValues);
                result.add(row);
            }
        }
        return result;
    }


    /**
     * sql执行结果
     * @param sql
     * @throws SQLException
     */
    public Map<String,Row> execSQL(String sql, String schema, String pk) throws SQLException {
        Connection conn = getConnection();
        PreparedStatement ppst = null;
        ResultSet rs = null;
        Map<String,Row> result = new HashMap<String,Row>();
        if(null != conn){
            String exeSchema = schema.replaceAll("\\s*", "");
            ppst = conn.prepareStatement(sql);
            rs = ppst.executeQuery();
            while(rs.next()){
                List<Object> rowValues = new ArrayList<Object>();
                String[] fieldNames = exeSchema.split(QRealTimeConstant.COMMA());
                String pkKey = rs.getObject(pk).toString();
                for(String fieldName : fieldNames){
                    Object fValue = rs.getObject(fieldName);
                    rowValues.add(fValue);
                }
                Row row = Row.of(rowValues.toArray());
                result.put(pkKey, row);
            }
        }
        return result;
    }


    /**
     * sql执行结果
     * @param sql
     * @throws SQLException
     */
    public Map<String,String> execSQLJson(String sql, String schema, String pk) throws SQLException {
        Connection conn = getConnection();
        PreparedStatement ppst = null;
        ResultSet rs = null;
        Map<String,String> result = new HashMap<String,String>();
        if(null != conn){
            String exeSchema = schema.replaceAll("\\s*", "");
            ppst = conn.prepareStatement(sql);
            rs = ppst.executeQuery();
            Map<String,Object> rowValues = new HashMap<String,Object>();
            while(rs.next()){
                String[] fieldNames = exeSchema.split(QRealTimeConstant.COMMA());
                String pkKey = rs.getObject(pk).toString();
                for(String fieldName : fieldNames){
                    Object fValue = rs.getObject(fieldName);
                    rowValues.put(fieldName, fValue);
                }
                String rowJson = JsonUtil.gObject2Json(rowValues);
                //System.out.println("rowJson===>" + rowJson);
                result.put(pkKey, rowJson);
                rowValues.clear();
            }
        }
        return result;
    }


    //测试
    public static void main(String[] args) throws Exception{

        Properties dbProperties = PropertyUtil.readProperties("jdbc.properties");

        String driver = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_DRIVER_MYSQL_KEY());
        String url  = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_URL_KEY());
        String user = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_USERNAME_KEY());
        String passwd = dbProperties.getProperty(QRealTimeConstant.FLINK_JDBC_PASSWD_KEY());

        DBDruid dbDruid = new DBDruid(driver, url, user, passwd);
        String sql = "select product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type from dim_product1";
        String schema = "product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type";
        String pk = "product_id";

        Map<String,String> results2 = dbDruid.execSQLJson(sql, schema, pk);
        for(Map.Entry<String,String> entry : results2.entrySet()){
            String key = entry.getKey();
            String value = entry.getValue();
            Map<String,Object> row = JsonUtil.json2object(value, Map.class);

            //String values = entry.getValue();
            //Row value = JsonUtil.json2object(values, Row.class);
            String productID = row.get("product_id").toString();
            String productLevel = row.get("product_level").toString();
            String productType = row.get("product_type").toString();
            String toursimType = row.get("toursim_tickets_type").toString();
            System.out.println("productID=>" + productID);
        }
    }
}
