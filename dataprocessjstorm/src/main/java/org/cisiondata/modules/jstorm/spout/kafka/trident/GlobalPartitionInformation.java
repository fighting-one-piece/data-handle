package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.cisiondata.modules.jstorm.spout.kafka.Broker;
import org.cisiondata.modules.jstorm.spout.kafka.Partition;

import com.google.common.base.Objects;

@SuppressWarnings("serial")
public class GlobalPartitionInformation implements Iterable<Partition>, Serializable {

    private Map<Integer, Broker> partitionMap;
    public String topic;

    //Flag to keep the Partition Path Id backward compatible with Old implementation of Partition.getId() == "partition_" + partition
    private Boolean bUseTopicNameForPartitionPathId;

    public GlobalPartitionInformation(String topic, Boolean bUseTopicNameForPartitionPathId) {
        this.topic = topic;
        this.partitionMap = new TreeMap<Integer, Broker>();
        this.bUseTopicNameForPartitionPathId = bUseTopicNameForPartitionPathId;
    }

    public GlobalPartitionInformation(String topic) {
        this.topic = topic;
        this.partitionMap = new TreeMap<Integer, Broker>();
        this.bUseTopicNameForPartitionPathId = false;
    }

    public void addPartition(int partitionId, Broker broker) {
        partitionMap.put(partitionId, broker);
    }

    public Boolean getbUseTopicNameForPartitionPathId () {
        return bUseTopicNameForPartitionPathId;
    }

    @Override
    public String toString() {
        return "GlobalPartitionInformation{" +
                "topic=" + topic +
                ", partitionMap=" + partitionMap +
                '}';
    }

    public Broker getBrokerFor(Integer partitionId) {
        return partitionMap.get(partitionId);
    }

    public List<Partition> getOrderedPartitions() {
        List<Partition> partitions = new LinkedList<Partition>();
        for (Map.Entry<Integer, Broker> partition : partitionMap.entrySet()) {
            partitions.add(new Partition(partition.getValue(), this.topic, partition.getKey(), this.bUseTopicNameForPartitionPathId));
        }
        return partitions;
    }

    @Override
    public Iterator<Partition> iterator() {
        final Iterator<Map.Entry<Integer, Broker>> iterator = partitionMap.entrySet().iterator();
        final String topic = this.topic;
        final Boolean bUseTopicNameForPartitionPathId = this.bUseTopicNameForPartitionPathId;
        return new Iterator<Partition>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Partition next() {
                Map.Entry<Integer, Broker> next = iterator.next();
                return new Partition(next.getValue(), topic , next.getKey(), bUseTopicNameForPartitionPathId);
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitionMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final GlobalPartitionInformation other = (GlobalPartitionInformation) obj;
        return Objects.equal(this.partitionMap, other.partitionMap);
    }
}
