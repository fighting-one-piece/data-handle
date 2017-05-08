package org.cisiondata.modules.jstorm.spout.kafka.trident.selector;

import storm.trident.tuple.TridentTuple;

@SuppressWarnings("serial")
public class DefaultTopicSelector implements KafkaTopicSelector {

    private final String topicName;

    public DefaultTopicSelector(final String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String getTopic(TridentTuple tuple) {
        return topicName;
    }
}
