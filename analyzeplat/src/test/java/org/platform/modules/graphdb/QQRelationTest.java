package org.platform.modules.graphdb;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.platform.utils.QQRelationUtils;
import org.platform.utils.file.DefaultLineHandler;
import org.platform.utils.file.FileUtils;

public class QQRelationTest {
	
	public static void produce(List<String> messages) {
		Properties properties = new Properties();  
        properties.put("bootstrap.servers", "192.168.0.15:9092,192.168.0.16:9092,192.168.0.17:9092");  
        properties.put("producer.type", "sync");  
        properties.put("request.required.acks", "1");  
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");  
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 1, len = messages.size(); i < len; i++) {
        	String message = messages.get(i);
        	producer.send(new ProducerRecord<String, String>("qqnode", i % 6, "" + i, message), new Callback() {
        		
        		@Override
        		public void onCompletion(RecordMetadata metadata, Exception exception) {
        			System.out.println(metadata.offset() + ":" + metadata.topic() + ":" 
        					+ metadata.partition());
        		}
        	});
        }
		producer.close(); 
	}
	
	public static void insertQQNode() {
		try {
			List<String> lines = FileUtils.readFromAbsolute("F:\\document\\doc\\201704\\qqdata", new DefaultLineHandler());
//			for (String line : lines) {
//				QQRelationUtils.insertQQNode(line);
//			}
			produce(lines);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void insertQunNode() {
		try {
			List<String> lines = FileUtils.readFromAbsolute("F:\\document\\doc\\201704\\qqqundata", new DefaultLineHandler());
			for (String line : lines) {
				QQRelationUtils.insertQunNode(line);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		insertQQNode();
	}
	
}
