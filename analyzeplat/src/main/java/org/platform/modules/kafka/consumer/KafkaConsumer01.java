package org.platform.modules.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer01 {
	
	private ConsumerConnector consumer = null;
    private String topic = null;
    private ExecutorService executor = null;
 
    public KafkaConsumer01(String zookeeper, String group_id, String topic) {
//        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        consumer = createConsumerConnector(zookeeper, group_id);
        this.topic = topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
   }
 
    public void run(int thread_num) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(thread_num));
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topic);
 
        executor = Executors.newFixedThreadPool(thread_num);
 
        int thread_number_flag = 0;
        for (KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new ConsumerRunnable(stream, thread_number_flag));
            thread_number_flag++;
        }
    }
 
    public ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("group.id", a_groupId);
        return new ConsumerConfig(props);
    }
    
    public ConsumerConnector createConsumerConnector(String a_zookeeper, String a_groupId) {
		Properties properties = new Properties();
		// zookeeper配置
		properties.put("zookeeper.connect", a_zookeeper);
		properties.put("zookeeper.session.timeout.ms", "400");
		properties.put("zookeeper.sync.time.ms", "200");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
		properties.put("group.id", a_groupId);
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(
				properties));
	}
 
    public static void main(String[] args) {
        String zookeeper = "192.168.0.115:2181/kafka";
        String groupId = "test-consumer-group";
        String topic = "kafka-elasticsearch-04";
        int threads_num = 1;
 
        KafkaConsumer01 kafka_consumer = new KafkaConsumer01(zookeeper, groupId, topic);
        kafka_consumer.run(threads_num);
 
        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {
 
        }
        kafka_consumer.shutdown();
    }
}

class ConsumerRunnable implements Runnable {
	
	private KafkaStream<byte[], byte[]> kafka_stream = null;
    private int current_thread_number;
 
    public ConsumerRunnable(KafkaStream<byte[], byte[]> stream, int thread_number) {
        this.kafka_stream = stream;
        this.current_thread_number = thread_number;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> iterator = kafka_stream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("Thread " + current_thread_number + " 接收到: " + message);
		}
        System.out.println("Shutting Down Thread: " + current_thread_number);
    }
}