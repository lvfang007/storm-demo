package com.lvfang.storm;

import org.apache.commons.lang.time.FastDateFormat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @Author: LvFang
 * @Date: Created in 2018/9/21.
 * @Description:
 */
public class LogUtil {

    public static final FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

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
    public static String getLog(){
        Random random = new Random();

        int id = random.nextInt(100000000);
        int userId = random.nextInt(100000);
        int price = random.nextInt(100)+10;
        int youhui = random.nextInt(20);
        int status = random.nextInt(3);

        StringBuffer message = new StringBuffer();
        message.append(String.valueOf(id))
                .append(" ")
                .append(String.valueOf(userId))
                .append(" ")
                .append(String.valueOf(price))
                .append(" ")
                .append(String.valueOf(youhui))
                .append(" ")
                .append(String.valueOf(status))
                .append(" ")
                .append(new Date().getTime()-1000*60*60*24*random.nextInt(10));

        return message.toString();
    }

    public static final SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Long getSignTime(String timeStr) throws Exception{

        return sim.parse(timeStr).getTime();
    }
}
