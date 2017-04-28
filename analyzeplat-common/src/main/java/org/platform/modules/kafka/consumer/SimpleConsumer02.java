package org.platform.modules.kafka.consumer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer02 {

	public static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer02.class);
	
	public void consume() {
		Properties properties = new Properties();  
		properties.put("bootstrap.servers", "192.168.0.115:9092");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test-consumer-group");
		properties.put("broker.id", "1");
		properties.put("log.dirs", "/home/ym/Software/kafka-2.11-0.10.0.0/logs/");
		properties.put("zookeeper.connect", "192.168.0.115:2181/kafka");
		properties.put("zookeeper.session.timeout.ms", "400");
		properties.put("zookeeper.sync.time.ms", "200");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList("kafka-test"));
		boolean isRunning = true;
		while (isRunning) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			System.out.println("records count: " + records.count());
			if (!records.isEmpty()) {
				Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
				ConsumerRecord<String, String> record = null;
				while (iterator.hasNext()) {
					record = iterator.next();
					System.out.println(record.topic() + ":" + record.offset() 
							+ ":" + record.key() + ":" + record.value());
				}
			} else {
				isRunning = false;
			}
		}
		consumer.close();
	}

	public static void main(String[] args) {
		new SimpleConsumer02().consume();
	}
	
}
