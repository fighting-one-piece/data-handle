package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.List;

public interface IBrokerReader {

    GlobalPartitionInformation getBrokerForTopic(String topic);

    List<GlobalPartitionInformation> getAllBrokers();

    void close();
}
