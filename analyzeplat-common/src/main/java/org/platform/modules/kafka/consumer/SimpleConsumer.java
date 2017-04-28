package org.platform.modules.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleConsumer extends Thread {

	private String topic;

	public SimpleConsumer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		ConsumerConnector consumer = createConsumerConnector();
		// 一次从主题中获取数据数量
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("接收到: " + message);
		}
	}

	private ConsumerConnector createConsumerConnector() {
		Properties properties = new Properties();
		// zookeeper配置
		properties.put("zookeeper.connect", "192.168.0.15:2181,192.168.0.16:2181,192.168.0.17:2181/kafka");
		properties.put("zookeeper.session.timeout.ms", "400");
		properties.put("zookeeper.sync.time.ms", "200");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
		properties.put("group.id", "cisiondata-consumer-group");
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(
				properties));
	}

	public static void main(String[] args) {
		new SimpleConsumer("elastic5").start();// 使用kafka集群中创建好的主题 test
	}

}
