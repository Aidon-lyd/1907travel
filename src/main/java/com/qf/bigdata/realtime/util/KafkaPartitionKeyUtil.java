package com.qf.bigdata.realtime.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
 * kafka自定义分区
 */
public class KafkaPartitionKeyUtil implements Partitioner {

    private Random random = new Random();

    public void close() {
    }

    public void configure(Map<String, ?> configs) {
    }

    //使用其来分区，返回分区编号
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取kafka的主题的分区
        int numPartitions = cluster.partitionCountForTopic(topic);

        int position = 0;
        if(null == value){
            position = numPartitions-1;
        }else{
            //value不等于null，，使用key的hashcode()%numPartitions=分区编号
            String partitionInfo = key.toString();
            Integer num = Math.abs(partitionInfo.hashCode());
            //Long num = Long.valueOf(partitionInfo);
            Integer pos = num % numPartitions;
            position = pos.intValue();
            System.out.println("data partitions is " + position + ",num=" + num + ",numPartitions=" + numPartitions);
        }
        return position;
    }

    public static void main(String[] args) throws  Exception{
        System.out.println("aaaaaaaa");
    }
}
