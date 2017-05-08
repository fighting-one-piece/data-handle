package org.cisiondata.modules.jstorm.spout.kafka.trident.mapper;

import storm.trident.tuple.TridentTuple;

@SuppressWarnings({ "rawtypes", "serial" })
public class FieldNameBasedTupleToKafkaMapper<K, V> implements TridentTupleToKafkaMapper {

    public final String keyFieldName;
    public final String msgFieldName;

    public FieldNameBasedTupleToKafkaMapper(String keyFieldName, String msgFieldName) {
        this.keyFieldName = keyFieldName;
        this.msgFieldName = msgFieldName;
    }

    @SuppressWarnings("unchecked")
	@Override
    public K getKeyFromTuple(TridentTuple tuple) {
        return (K) tuple.getValueByField(keyFieldName);
    }

    @SuppressWarnings("unchecked")
	@Override
    public V getMessageFromTuple(TridentTuple tuple) {
        return (V) tuple.getValueByField(msgFieldName);
    }
}
