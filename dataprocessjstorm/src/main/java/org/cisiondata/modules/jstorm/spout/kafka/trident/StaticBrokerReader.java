package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class StaticBrokerReader implements IBrokerReader {

    private Map<String,GlobalPartitionInformation> brokers = new TreeMap<String,GlobalPartitionInformation>();

    public StaticBrokerReader(String topic, GlobalPartitionInformation partitionInformation) {
        this.brokers.put(topic, partitionInformation);
    }

    @Override
    public GlobalPartitionInformation getBrokerForTopic(String topic) {
        if (brokers.containsKey(topic)) return brokers.get(topic);
        return null;
    }

    @Override
    public List<GlobalPartitionInformation> getAllBrokers () {
        List<GlobalPartitionInformation> list = new ArrayList<GlobalPartitionInformation>();
        list.addAll(brokers.values());
        return list;
    }

    @Override
    public void close() {
    }
}
