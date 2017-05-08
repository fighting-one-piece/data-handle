package org.cisiondata.modules.jstorm.spout.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class IntSerializer implements Serializer<Integer> {
  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String topic, Integer val) {
    return new byte[] {
            (byte) (val >>> 24),
            (byte) (val >>> 16),
            (byte) (val >>> 8),
            val.byteValue()
        };
  }

  @Override
  public void close() {
  }
}
