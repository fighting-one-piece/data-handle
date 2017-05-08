package org.cisiondata.modules.jstorm.spout.kafka;

import java.util.*;

import org.cisiondata.modules.jstorm.spout.kafka.trident.GlobalPartitionInformation;

public class StaticCoordinator implements PartitionCoordinator {
    Map<Partition, PartitionManager> _managers = new HashMap<Partition, PartitionManager>();
    List<PartitionManager> _allManagers = new ArrayList<>();

    @SuppressWarnings("rawtypes")
	public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        StaticHosts hosts = (StaticHosts) config.hosts;
        List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();
        partitions.add(hosts.getPartitionInformation());
        List<Partition> myPartitions = KafkaUtils.calculatePartitionsForTask(partitions, totalTasks, taskIndex);
        for (Partition myPartition : myPartitions) {
            _managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, state, stormConf, config, myPartition));
        }
        _allManagers = new ArrayList<>(_managers.values());
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        return _allManagers;
    }

    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }

    @Override
    public void refresh() { return; }

}
