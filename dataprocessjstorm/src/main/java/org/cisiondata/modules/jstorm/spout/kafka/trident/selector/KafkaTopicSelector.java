package org.cisiondata.modules.jstorm.spout.kafka.trident.selector;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

public interface KafkaTopicSelector extends Serializable {
	
    String getTopic(TridentTuple tuple);
}
