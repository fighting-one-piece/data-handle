package org.cisiondata.modules.jstorm.spout.kafka;

//import static backtype.storm.utils.Utils.tuple;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class StringScheme implements Scheme {
	
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
    public static final String STRING_SCHEME_KEY = "str";
    public static final String BYTE_SCHEME_KEY = "bytes";
    
    public List<Object> deserialize(ByteBuffer bytes) {
        return new Values(deserializeString(bytes));
    }

    public static String deserializeString(ByteBuffer string) {
        if (string.hasArray()) {
            int base = string.arrayOffset();
            return new String(string.array(), base + string.position(), string.remaining(), UTF8_CHARSET);
        } else {
            return new String(Utils.toByteArray(string), UTF8_CHARSET);
        }
    }
    
	@Override
	public List<Object> deserialize(byte[] ser) {
		return new Values(deserializeString(ser));
//		return tuple(ser);
//		return new Values(new String(ser, UTF8_CHARSET));
	}
    
    public static String deserializeString(byte[] ser) {
    	return deserializeString(ByteBuffer.wrap(ser));
//    	return new Values(new String(ser, UTF8_CHARSET));
    }
    
    public Fields getOutputFields() {
        return new Fields(BYTE_SCHEME_KEY);
    }

}
