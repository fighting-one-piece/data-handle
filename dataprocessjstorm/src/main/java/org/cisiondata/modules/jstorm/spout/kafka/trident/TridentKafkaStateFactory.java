package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.Map;
import java.util.Properties;

import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.StateFactory;

@SuppressWarnings("serial")
public class TridentKafkaStateFactory implements StateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaStateFactory.class);

    @SuppressWarnings("rawtypes")
	private TridentTupleToKafkaMapper mapper;
    private KafkaTopicSelector topicSelector;
    private Properties producerProperties = new Properties();

    @SuppressWarnings("rawtypes")
	public TridentKafkaStateFactory withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaStateFactory withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    public TridentKafkaStateFactory withProducerProperties(Properties props) {
        this.producerProperties = props;
        return this;
    }


    @SuppressWarnings("rawtypes")
	@Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        TridentKafkaState state = new TridentKafkaState()
                .withKafkaTopicSelector(this.topicSelector)
                .withTridentTupleToKafkaMapper(this.mapper);
        state.prepare(producerProperties);
        return null;
    }
}
