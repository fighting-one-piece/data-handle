package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import backtype.storm.utils.Utils;

public class ByteBufferSerializer implements Serializer<ByteBuffer> {
	
	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public void close() {

	}

	@Override
	public byte[] serialize(String s, ByteBuffer b) {
		return Utils.toByteArray(b);
	}
}
