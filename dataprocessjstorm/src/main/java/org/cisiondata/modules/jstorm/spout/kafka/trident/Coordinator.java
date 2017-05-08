package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.List;
import java.util.Map;

import org.cisiondata.modules.jstorm.spout.kafka.KafkaUtils;

import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;

class Coordinator implements IPartitionedTridentSpout.Coordinator<List<GlobalPartitionInformation>>, IOpaquePartitionedTridentSpout.Coordinator<List<GlobalPartitionInformation>> {

    private IBrokerReader reader;
    private TridentKafkaConfig config;

    @SuppressWarnings("rawtypes")
	public Coordinator(Map conf, TridentKafkaConfig tridentKafkaConfig) {
        config = tridentKafkaConfig;
        reader = KafkaUtils.makeBrokerReader(conf, config);
    }

    @Override
    public void close() {
        config.coordinator.close();
    }

    @Override
    public boolean isReady(long txid) {
        return config.coordinator.isReady(txid);
    }

    @Override
    public List<GlobalPartitionInformation> getPartitionsForBatch() {
        return reader.getAllBrokers();
    }
}
