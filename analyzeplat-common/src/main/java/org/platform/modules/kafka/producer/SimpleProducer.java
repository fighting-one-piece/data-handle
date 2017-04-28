package org.platform.modules.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleProducer {
	
	public void produce() {
		Properties properties = new Properties();  
        properties.put("bootstrap.servers", "192.168.0.15:9092,192.168.0.16:9092,192.168.0.17:9092");  
        properties.put("producer.type", "sync");  
        properties.put("request.required.acks", "1");  
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");  
        properties.put("partitioner.class", "kafka.producer.DefaultPartitioner");  
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 1; i <= 1000; i++) {
        	producer.send(new ProducerRecord<String, String>("elastic5", "msgId" + i, "msg" + i), new Callback() {
        		
        		@Override
        		public void onCompletion(RecordMetadata metadata, Exception exception) {
        			System.out.println(metadata.offset() + ":" + metadata.topic() + ":" 
        					+ metadata.partition());
        		}
        	});
        }
		producer.close(); 
	}

	public static void main(String[] args) {
		new SimpleProducer().produce();
	}
	
}
