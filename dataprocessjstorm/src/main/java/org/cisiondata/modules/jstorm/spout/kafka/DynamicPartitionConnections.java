package org.cisiondata.modules.jstorm.spout.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cisiondata.modules.jstorm.spout.kafka.trident.IBrokerReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.consumer.SimpleConsumer;

public class DynamicPartitionConnections {

    @SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionConnections.class);

    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<String> partitions = new HashSet<String>();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }

    Map<Broker, ConnectionInfo> _connections = new HashMap<>();
    KafkaConfig _config;
    IBrokerReader _reader;

    public DynamicPartitionConnections(KafkaConfig config, IBrokerReader brokerReader) {
        _config = config;
        _reader = brokerReader;
    }

    public SimpleConsumer register(Partition partition) {
        Broker broker = _reader.getBrokerForTopic(partition.topic).getBrokerFor(partition.partition);
        return register(broker, partition.topic, partition.partition);
    }

    public SimpleConsumer register(Broker host, String topic, int partition) {
        if (!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleConsumer(host.host, host.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId)));
        }
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(getHashKey(topic,partition));
        return info.consumer;
    }

    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = _connections.get(partition.host);
        if (info != null) {
            return info.consumer;
        }
        return null;
    }

    public void unregister(Broker port, String topic, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(getHashKey(topic,partition));
        if (info.partitions.isEmpty()) {
            info.consumer.close();
            _connections.remove(port);
        }
    }

    public void unregister(Partition partition) {
        unregister(partition.host, partition.topic, partition.partition);
    }

    public void clear() {
        for (ConnectionInfo info : _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }

    private String getHashKey(String topic, int partition) {
        return topic + "_" + partition;
    }
}
