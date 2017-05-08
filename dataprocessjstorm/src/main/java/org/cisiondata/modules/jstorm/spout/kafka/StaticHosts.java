package org.cisiondata.modules.jstorm.spout.kafka;

import org.cisiondata.modules.jstorm.spout.kafka.trident.GlobalPartitionInformation;

@SuppressWarnings("serial")
public class StaticHosts implements BrokerHosts {

    private GlobalPartitionInformation partitionInformation;

    public StaticHosts(GlobalPartitionInformation partitionInformation) {
        this.partitionInformation = partitionInformation;
    }

    public GlobalPartitionInformation getPartitionInformation() {
        return partitionInformation;
    }
}
