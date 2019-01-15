package com.lvfang.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @Author: LvFang
 * @Date: Created in 2018/10/9.
 * @Description:计算结果 bolt
 */
public class CountBolt  extends BaseBasicBolt {

    private static final long serialVersionUID = 1l;

    private static final Logger logger = LoggerFactory.getLogger(CountBolt.class);

    //订单个数
    private static volatile Integer orderCount = 0;
    //优惠前总订单价格
    private static volatile Long sumPrice = 0l;
    //优惠后总订单价格
    private static volatile Long sumOldPrice = 0l;
    //参与活动的用户集合
    private static volatile Set<String> userSet = new HashSet<String>();

    public static Jedis jedis;

    static {
//        jedis = new Jedis("192.168.90.201",6379,5000);
        //jedis.auth("cjqc123456");

        jedis = new Jedis("10.168.99.149",6379,5000);
        //jedis.auth("cjqc123456");
    }

    /**
     * 订单号       用户id        原价      优惠价     标示字段        下单时间
     * id          userId      price       youhui      status      date
     *
     * 求订单个数：消息次数（或者付款次数）
     * 求参与的用户数：
     * 求总原价：
     * 求总优惠价：
     *
     */

    public void execute(Tuple input, BasicOutputCollector collector) {
        String message = input.getStringByField("count_bolt");//(String) input.getValue(0);
        try {
            if(StringUtils.isNotBlank(message)){
                String[] messages = message.split(" ");
                int id = Integer.parseInt(messages[0]);//订单id
                String userId = messages[1];//用户id
                int price = Integer.parseInt(messages[2]);//价格
                int youhui = Integer.parseInt(messages[3]);//优惠后价格

                sumPrice += price;
                sumOldPrice +=youhui;
                userSet.add(userId);
                orderCount++;
            }

        }catch (Exception e){
            //异常数据处理
            logger.info("异常数据,ACK处理：{}",message);
        }


//        logger.info("sumPrice {} ",sumPrice.toString());
//        logger.info("youhui {} ",sumOldPrice.toString());
//        logger.info("userSet Size {} ",userSet.size());

        jedis.hset("storm_test","优惠前总价",sumPrice.toString());
        jedis.hset("storm_test","优惠后总价",sumOldPrice.toString());
        jedis.hset("storm_test","活动用户数",String.valueOf(userSet.size()));
        jedis.hset("storm_test","活动订单数",String.valueOf(orderCount));

    }

    //声明数据id
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        //declarer.declare(new Fields("count_bolt"));
    }
}
