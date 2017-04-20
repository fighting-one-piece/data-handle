package org.platform.modules.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer02 {
	
	public void produce() {
		Properties properties = new Properties();  
        properties.put("bootstrap.servers", "192.168.0.115:9092");  
        properties.put("producer.type", "sync");  
        properties.put("request.required.acks", "1");  
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");  
//        properties.put("partitioner.class", "kafka.producer.DefaultPartitioner");  
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");  
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"); 
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(properties);
        for (int i = 1; i <= 20; i++) {
        	producer.send(new ProducerRecord<byte[], byte[]>("kafka-test", 
        			new String("msgId" + i).getBytes(), new String("msg" + i).getBytes()), 
        			new Callback() {
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
		new Producer02().produce();
	}
	
}
