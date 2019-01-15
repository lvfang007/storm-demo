package com.lvfang.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @Author: LvFang
 * @Date: Created in 2018/9/26.
 * @Description:状态修正bolt
 */
public class StatusUpdateBolt  extends BaseBasicBolt {

    private static final long serialVersionUID = 1l;

    private static final Logger logger = LoggerFactory.getLogger(StatusUpdateBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        String message = input.getStringByField("status_update_bolt");//(String) input.getValue(0);

        int lastIndex = message.split(" ").length;
        String status = message.split(" ")[lastIndex-2];
        //假设我们0或者1表示成功的订单状态（0：已支付 1：交易完成）
        if("0".equals(status) || "1".equals(status)){
            logger.info("成功状态订单：{}",message);
            collector.emit(new Values(message));
        }else{
            logger.info("失败状态订单：{}",message);
        }
    }

    //声明数据id
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count_bolt"));
    }
}
