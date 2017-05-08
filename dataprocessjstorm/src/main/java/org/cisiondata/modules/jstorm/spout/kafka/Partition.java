package org.cisiondata.modules.jstorm.spout.kafka;

import java.io.Serializable;

import com.google.common.base.Objects;

import storm.trident.spout.ISpoutPartition;

@SuppressWarnings("serial")
public class Partition implements ISpoutPartition, Serializable {

    public Broker host;
    public int partition;
    public String topic;

    //Flag to keep the Partition Path Id backward compatible with Old implementation of Partition.getId() == "partition_" + partition
    private Boolean bUseTopicNameForPartitionPathId;

    // for kryo compatibility
    @SuppressWarnings("unused")
	private Partition() {
    }
    
    public Partition(Broker host, String topic, int partition) {
        this.topic = topic;
        this.host = host;
        this.partition = partition;
        this.bUseTopicNameForPartitionPathId = false;
    }
    
    public Partition(Broker host, String topic, int partition,Boolean bUseTopicNameForPartitionPathId) {
        this.topic = topic;
        this.host = host;
        this.partition = partition;
        this.bUseTopicNameForPartitionPathId = bUseTopicNameForPartitionPathId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, topic, partition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Partition other = (Partition) obj;
        return Objects.equal(this.host, other.host) && Objects.equal(this.topic, other.topic) && Objects.equal(this.partition, other.partition);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "host=" + host +
                ", topic=" + topic +
                ", partition=" + partition +
                '}';
    }

    @Override
    public String getId() {
        if (bUseTopicNameForPartitionPathId) {
            return  topic  + "/partition_" + partition;
        } else {
            //Keep the Partition Id backward compatible with Old implementation of Partition.getId() == "partition_" + partition
            return "partition_" + partition;
        }
    }

}
