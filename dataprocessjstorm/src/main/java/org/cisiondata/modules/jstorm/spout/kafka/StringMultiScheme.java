package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class StringMultiScheme implements MultiScheme {
	
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
    public static final String STRING_SCHEME_KEY = "str";
    public static final String BYTE_SCHEME_KEY = "bytes";
    
    @Override
	public Iterable<List<Object>> deserialize(byte[] ser) {
		return null;
	}
    
    public Iterable<List<Object>> deserialize(ByteBuffer bytes) {
        return Collections.singletonList(new Values(deserializeString(bytes)));
    }

    public static String deserializeString(ByteBuffer string) {
        if (string.hasArray()) {
            int base = string.arrayOffset();
            return new String(string.array(), base + string.position(), string.remaining(), UTF8_CHARSET);
        } else {
            return new String(Utils.toByteArray(string), UTF8_CHARSET);
        }
    }
    
    public Fields getOutputFields() {
        return new Fields(BYTE_SCHEME_KEY);
    }

}
