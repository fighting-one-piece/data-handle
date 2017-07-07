package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class BaseHDFS2ES5V1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2ES5V1Mapper.class);
	
	private Gson gson = null;
	
	private String esIndex = null;
	
	private String esType = null;
	
	private String topic = null;
	
	private KafkaProducer<String, String> producer = null;
	
	private static final int BATCH = 1200;

	private static final int PARTITION_NUM = 12;

	private static final String DEFAULT_TOPIC = "elastic5";
	
	private long totalLines = 0L;
	
	private Map<String, String> messages = new HashMap<String, String>(BATCH);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
				.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.esIndex = (String) context.getConfiguration().get("esIndex");
		this.esType = (String) context.getConfiguration().get("esType");
		this.topic = (String) context.getConfiguration().get("topic", DEFAULT_TOPIC);
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

	@SuppressWarnings("unchecked")
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			Map<String, Object> data = gson.fromJson(value.toString(), Map.class);
			data.put("index", esIndex);
			data.put("type", esType);
			messages.put(String.valueOf(data.get("_id")), convertToMessage(data));
			if (messages.size() == BATCH) {
				sendMessages(messages);
				messages.clear();
			}
			totalLines++;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
		if (messages.size() > 0) sendMessages(messages);
		producer.close();
		context.write(new Text("lines"), new LongWritable(totalLines));
	}
	
	private void sendMessages(Map<String, String> messages) {
		int index = 0;
		for (Map.Entry<String, String> entry : messages.entrySet()) {
			producer.send(new ProducerRecord<String, String>(this.topic, 
					index % PARTITION_NUM, entry.getKey(), entry.getValue()));
			index++;
		}
	}
	
}

