package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.List;

import backtype.storm.spout.Scheme;

public interface MessageMetadataScheme extends Scheme {
	
    List<Object> deserializeMessageWithMetadata(ByteBuffer message, Partition partition, long offset);
}
