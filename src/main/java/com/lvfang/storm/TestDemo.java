package com.lvfang.storm;/**
 * Created by lvfang on 2018/9/20.
 */

import org.junit.Test;
import utils.ResourceUtils;

import java.util.Random;

/**
 * Created with lvfang.
 * User: lvfang
 * Date: 2018/9/20
 * Time: 23:23
 * Desc:
 */
public class TestDemo {

    // 主题与zk端口(local)
    public static final Integer ZK_PORT = Integer.parseInt(ResourceUtils.getProperty("zookeeper.port"));
    public static final String ZK_HOST = ResourceUtils.getProperty("zookeeper.host");
    public static final String ZKINFO = ResourceUtils.getProperty("zookeeper.connect.kafka");

    public static final String KAFKA_URL = ResourceUtils.getProperty("kafka.broker");
    public static final String ZKROOT = "/hdtas";
    public static final String SPOUTID = "hdtas";

    private static final String HDTAS_SPOUT = "hdtasSpout";
    private static final String HDTAS_ALL_BOLT = "hdtas_all_bolt";
    /*
    * 需求：
    *
    * select
    *   count(id)有效订单个数
    *   sum(price)优惠前金额
    *   sum(price - youhui)优惠后金额
    *   count(distinct userId) 下单用户数（第三个bolt进行计算，分析，存储）
    *   case when substring(status,9,1)='1' then 1 when substring(status,9,1)='2' then 2 else -1 end
    * from
    *   realtime_orders
    * where
    *   date >= '2018-09-09' (第一个bolt进行订单的有效性日期判断)
    * group by
    *   case when substring(status,9,1)='1' then 1 when substring(status,9,1)='2' then 2 else -1 end （第二个bolt进行状态纠正）
    *
    *
    * storm + kafka + memcached/redis + MySQL + zookeeper
    *
    * 1、web端录入日志或者直接发送消息至MQ
    * 2、storm整合kafka消费消息
    * 3、业务计算并持久化
    *
    *
    * message(消息)
    * 订单号       用户id        原价      优惠价     标示字段        下单时间
    * id          userId      price       youhui      status      date
    *
    * */

    /**
     * psvm
     * psout
     * @param args
     */
    public static void main(String[] args) {
        Random random = new Random();
        for(int i=0; i < 100; i++){
            int id = random.nextInt(100000000);
            int userId = random.nextInt(100000);
            int price = random.nextInt(1000)+10;
            int youhui = random.nextInt(100);
            int status = random.nextInt(3);

            StringBuffer data = new StringBuffer();
            data.append(String.valueOf(id))
                    .append(" ")
                    .append(String.valueOf(userId))
                    .append(" ")
                    .append(String.valueOf(price))
                    .append(" ")
                    .append(String.valueOf(youhui))
                    .append(" ")
                    .append(String.valueOf(status))
                    .append(" ")
                    .append("2018-09-09");

            System.out.println(data);
        }
    }

    @Test
    public void testResource(){
        System.out.println(ZK_PORT);
    }
}
