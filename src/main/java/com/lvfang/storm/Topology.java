package com.lvfang.storm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm07.PrinterBolt;
import storm07.SentenceBolt;
import utils.ResourceUtils;

import java.text.SimpleDateFormat;

/**
 * @Author: LvFang
 * @Date: Created in 2018/9/26.
 * @Description:
 */
public class Topology {
    private static final Logger logger = LoggerFactory.getLogger(Topology.class);

    private static final String ZK_CONNECT = ResourceUtils.getProperty("zookeeper.connect");
    public static final String KAFKA_TOPIC = "ARF";

    public static void main(String[] args) throws Exception{
        ZkHosts zkHosts = new ZkHosts(ZK_CONNECT);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,KAFKA_TOPIC,"","id-1");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true ;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("KafkaSpout",new KafkaSpout(kafkaConfig),1);
        builder.setBolt("TimeFilterBolt", new TimeFilterBolt(),1).globalGrouping("KafkaSpout");
        builder.setBolt("StatusUpdateBolt", new StatusUpdateBolt(),1).globalGrouping("TimeFilterBolt");
        builder.setBolt("CountBolt", new CountBolt(),1).globalGrouping("StatusUpdateBolt");


        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(20);

        if(args.length == 0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaTopology", conf, builder.createTopology());

            //如需要任务定时关闭，可以设置时长（一般来说storm的任务是不关闭的）
            //Thread.sleep(100000);
            //cluster.killTopology("KafkaTopology");
            //cluster.shutdown();
        }else{
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
