package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.platform.utils.json.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseHDFS2TitanV1Mapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2TitanV1Mapper.class);
	
	private String topic = null;
	
	private KafkaProducer<String, String> producer = null;
	
	private static final int BATCH = 600;

	private static final int PARTITION_NUM = 6;

	private Map<String, String> messages = new HashMap<String, String>(BATCH);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.topic = (String) context.getConfiguration().get("topic");
		Properties properties = new Properties();  
        properties.put("bootstrap.servers", "172.20.100.15:9092,172.20.100.16:9092,172.20.100.17:9092");  
        properties.put("producer.type", "sync");  
        properties.put("compression.codec", "1");
        properties.put("request.required.acks", "1");  
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");  
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
        this.producer = new KafkaProducer<String, String>(properties);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}
	
	public abstract String convertToMessage(Map<String, Object> data);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			Map<String, Object> data = GsonUtils.fromJsonToMap(value.toString());
			messages.put(String.valueOf(data.get("_id")), convertToMessage(data));
			if (messages.size() == BATCH) {
				sendMessages(messages);
				messages.clear();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
		if (messages.size() > 0) sendMessages(messages);
		producer.close();
	}
	
	private void sendMessages(Map<String, String> messages) {
		int index = 0;
		for (Map.Entry<String, String> entry : messages.entrySet()) {
			producer.send(new ProducerRecord<String, String>(topic, 
					index % PARTITION_NUM, entry.getKey(), entry.getValue()));
			index++;
		}
	}
	
}

