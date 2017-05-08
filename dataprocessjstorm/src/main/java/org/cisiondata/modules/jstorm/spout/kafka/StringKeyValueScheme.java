package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class StringKeyValueScheme extends StringScheme implements KeyValueScheme {

    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        if (key == null) {
            return deserialize(value);
        }
        String keyString = StringScheme.deserializeString(key);
        String valueString = StringScheme.deserializeString(value);
        return new Values(ImmutableMap.of(keyString, valueString));
    }

}
