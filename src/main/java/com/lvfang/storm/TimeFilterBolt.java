package com.lvfang.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.DemoUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: LvFang
 * @Date: Created in 2018/9/26.
 * @Description:按时间过滤订单数据bolt
 */
public class TimeFilterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1l;

    private static final Logger logger = LoggerFactory.getLogger(TimeFilterBolt.class);

    public static final String strTimeSign = "2019-01-06 00:00:00";



    private List<String> words = new ArrayList<String>();

    /**
     * 接收数据进行业务操作操作
     * @param input
     * @param collector
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String message = (String) input.getValue(0);
        try {
            if(StringUtils.isNotBlank(message)){
                int lastIndex = message.split(" ").length;
                Long orderTime = Long.parseLong(message.split(" ")[lastIndex-1]);
                //两天前订单：无效订单(比如说我们的双十一活动从2018-09-27)

                if(orderTime >= LogUtil.getSignTime(DemoUtils.getTimeStr())){
                    //做处理
                    logger.info("排除-无效订单：{}",message);
                    //有效订单
                }else{
                    logger.info("有效订单：{}",message);
                    collector.emit(new Values(message));
                }
            }
        }catch (Exception e){
            //异常数据处理
            logger.info("异常数据,ACK处理：{}",message);
        }
    }

    /**
     * 声明数据id
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status_update_bolt"));
    }

}
