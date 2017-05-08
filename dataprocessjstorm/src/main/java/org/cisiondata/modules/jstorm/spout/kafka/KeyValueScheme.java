package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.List;

import backtype.storm.spout.Scheme;

public interface KeyValueScheme extends Scheme {
	
    List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value);
}
