package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import backtype.storm.spout.SchemeAsMultiScheme;

@SuppressWarnings("serial")
public class KeyValueSchemeAsMultiScheme extends SchemeAsMultiScheme {

    public KeyValueSchemeAsMultiScheme(KeyValueScheme scheme) {
        super(scheme);
    }

    public Iterable<List<Object>> deserializeKeyAndValue(final ByteBuffer key, final ByteBuffer value) {
        List<Object> o = ((KeyValueScheme)scheme).deserializeKeyAndValue(key, value);
        if(o == null) return null;
        else return Arrays.asList(o);
    }

}
