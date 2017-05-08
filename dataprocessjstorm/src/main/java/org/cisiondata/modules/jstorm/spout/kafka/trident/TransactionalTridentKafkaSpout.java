package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.Map;

import org.cisiondata.modules.jstorm.spout.kafka.Partition;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.IPartitionedTridentSpout;

@SuppressWarnings({ "serial", "rawtypes" })
public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> {

    TridentKafkaConfig _config;

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    @SuppressWarnings({ "unchecked" })
	@Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new org.cisiondata.modules.jstorm.spout.kafka.trident.Coordinator(conf, _config);
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
	@Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new TridentKafkaEmitter(conf, context, _config, context.getStormId()).asTransactionalEmitter();
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
