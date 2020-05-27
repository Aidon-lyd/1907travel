package com.qf.bigdata.realtime.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;


/**
 * redis客户端工具类
 */
public class RedisCache implements Serializable {

    private static JedisPool pool = null;
    private static Jedis executor = null;

    private static final int port = 6379;
    private static final int timeout = 10 * 1000;
    private static final int maxIdle = 10;
    private static final int minIdle = 2;
    private static final int maxTotal = 20;

    private GenericObjectPoolConfig createConfig(){
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);
        return config;
    }

    public JedisPool connectRedisPool(String ip){
        if(null == pool){
            GenericObjectPoolConfig config = createConfig();
            pool = new JedisPool(config, ip, port, timeout);
        }
        return pool;
    }

    public Jedis connectRedis(String ip, int port, String auth){
        if(null == executor){
            executor = new Jedis(ip, port);
            executor.auth(auth);
        }
        return executor;
    }

    public static void main(String[] args) {

        String ip = "hadoop01";
        int port = 6379;
        String auth = "root";
        //获取连接
        RedisCache cache = new RedisCache();
        JedisPool pool = cache.connectRedisPool(ip);
        Jedis jedis = pool.getResource();
        jedis.auth(auth);

        //========================================
        String productTable = "travel.dim_product1";
        String productID = "210602273";

        jedis.hset(productTable,productID,"210153157\t115\tY002\t420100\t360400\t03001");

        String pubV = jedis.hget(productTable, productID);
        System.out.println("redis.product =" + productID + " , pubValue=" + pubV);
        /*Map<String,String> redisPubData = jedis.hgetAll(productTable);
        System.out.println("redis.product =" + redisPubData);*/
//        String productValue = redisPubData.get(productID);
//        System.out.println("redis.product =" + productID + " , pubValue=" + productValue);
    }
}
