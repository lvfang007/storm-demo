package com.lvfang.storm;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ResourceUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * kafka生产者类
 * @author lvfang
 *
 */
public class LogKafkaProduce extends Thread {

	private static final Logger logger = LoggerFactory.getLogger(LogKafkaProduce.class);

	public static final Random random = new Random();

	// 主题与zk端口(local)
	public static final String ZK_CONNECT = ResourceUtils.getProperty("zookeeper.connect");
	public static final String HOST_NAME = ResourceUtils.getProperty("kafka.host.name");
	public static final String KAFKA_BROKER_LIST = ResourceUtils.getProperty("kafka.broker");
	public static final String KAFKA_TOPIC = "ARF";

	private String topic;//主题

	public LogKafkaProduce(String topic){
        super();  
        this.topic = topic;  
    } 
	
	//创建生产者
	private Producer createProducer(){
		Properties properties = new Properties();
		
		//zookeeper单节点
		properties.put("zookeeper.connect",ZK_CONNECT);
		properties.put("serializer.class", StringEncoder.class.getName());  

		//kafka单节点
		properties.put("host.name", HOST_NAME);
		properties.put("advertised.host.name", HOST_NAME);
		properties.put("advertised.port", 9092);
	    properties.put("metadata.broker.list",KAFKA_BROKER_LIST);

	    return new Producer<Integer, String>(new ProducerConfig(properties)); 
	}
	
	@Override
	public void run() {
		//创建生产者
		Producer producer = createProducer();  
		String message ;
        //循环发送消息到kafka
        while(true){
			message = LogUtil.getLog();
			producer.send(new KeyedMessage<Integer, String>(topic, message ));
            try {  
            	//发送消息的时间间隔
//                TimeUnit.SECONDS.sleep(1);
				Thread.sleep(100*random.nextInt(20));
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }
			System.out.println(Thread.currentThread().getName() + "发送消息：" + message);
        }  
	}

	
	public static void main(String[] args) throws Exception {
		// 使用kafka集群中创建好的主题 test  
//		 new LogKafkaProduce(KAFKA_TOPIC).start();

		ExecutorService fxedThreadPool = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 5; i++) {
			fxedThreadPool.execute(new LogKafkaProduce(KAFKA_TOPIC));
		}

//		fxedThreadPool.shutdown();
	}

}

