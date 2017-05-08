package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class StringMultiSchemeWithTopic implements MultiScheme {
    public static final String STRING_SCHEME_KEY = "str";

    public static final String TOPIC_KEY = "topic";

    public Iterable<List<Object>> deserialize(ByteBuffer bytes) {
        throw new UnsupportedOperationException();
    }

    public Iterable<List<Object>> deserializeWithTopic(String topic, ByteBuffer bytes) {
        List<Object> items = new Values(StringScheme.deserializeString(bytes), topic);
        return Collections.singletonList(items);
    }

    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY, TOPIC_KEY);
    }

	@Override
	public Iterable<List<Object>> deserialize(byte[] ser) {
		List<Object> items = new Values(StringScheme.deserializeString(ser));
        return Collections.singletonList(items);
	}
}
