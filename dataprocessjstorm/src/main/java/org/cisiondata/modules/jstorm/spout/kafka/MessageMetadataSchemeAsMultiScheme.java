package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import backtype.storm.spout.SchemeAsMultiScheme;

public class MessageMetadataSchemeAsMultiScheme extends SchemeAsMultiScheme {
	
    private static final long serialVersionUID = -7172403703813625116L;

    public MessageMetadataSchemeAsMultiScheme(MessageMetadataScheme scheme) {
        super(scheme);
    }

    public Iterable<List<Object>> deserializeMessageWithMetadata(ByteBuffer message, Partition partition, long offset) {
        List<Object> o = ((MessageMetadataScheme) scheme).deserializeMessageWithMetadata(message, partition, offset);
        if (o == null) {
            return null;
        } else {
            return Arrays.asList(o);
        }
    }
}
