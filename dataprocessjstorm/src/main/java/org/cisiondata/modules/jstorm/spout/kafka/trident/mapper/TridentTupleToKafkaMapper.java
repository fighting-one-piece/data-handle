package org.cisiondata.modules.jstorm.spout.kafka.trident.mapper;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

public interface TridentTupleToKafkaMapper<K,V>  extends Serializable {
    K getKeyFromTuple(TridentTuple tuple);
    V getMessageFromTuple(TridentTuple tuple);
}
