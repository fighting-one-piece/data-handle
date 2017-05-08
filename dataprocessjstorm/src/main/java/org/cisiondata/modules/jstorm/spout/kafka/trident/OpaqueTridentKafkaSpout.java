package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.List;
import java.util.Map;

import org.cisiondata.modules.jstorm.spout.kafka.Partition;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

@SuppressWarnings({ "serial", "rawtypes" })
public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<List<GlobalPartitionInformation>, Partition, Map> {

    TridentKafkaConfig _config;

    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    @SuppressWarnings("deprecation")
	@Override
    public IOpaquePartitionedTridentSpout.Emitter<List<GlobalPartitionInformation>, Partition, Map> getEmitter(Map conf, TopologyContext context) {
        return new TridentKafkaEmitter(conf, context, _config, context
                .getStormId()).asOpaqueEmitter();
    }

    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext tc) {
        return new org.cisiondata.modules.jstorm.spout.kafka.trident.Coordinator(conf, _config);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
