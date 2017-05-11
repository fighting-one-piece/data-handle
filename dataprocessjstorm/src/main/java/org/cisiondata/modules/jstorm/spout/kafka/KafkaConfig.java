package org.cisiondata.modules.jstorm.spout.kafka;

import java.io.Serializable;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;
import kafka.api.FetchRequest;

public class KafkaConfig implements Serializable {
	
    private static final long serialVersionUID = 5276718734571623855L;
    
    public final BrokerHosts hosts;
    public final String topic;
    public final String clientId;

    public int fetchSizeBytes = 1024 * 1024;
    public int socketTimeoutMs = 10000;
    public int fetchMaxWait = 100;
    public int bufferSizeBytes = 1024 * 1024;
    public boolean ignoreZkOffsets = false;
    public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public long maxOffsetBehind = Long.MAX_VALUE;
    public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    public int metricsTimeBucketSizeInSecs = 60;
    public int minFetchByte = FetchRequest.DefaultMinBytes();
    public MultiScheme scheme = new RawMultiScheme();

    public KafkaConfig(BrokerHosts hosts, String topic) {
        this(hosts, topic, kafka.api.OffsetRequest.DefaultClientId());
    }

    public KafkaConfig(BrokerHosts hosts, String topic, String clientId) {
        this.hosts = hosts;
        this.topic = topic;
        this.clientId = clientId;
    }

}
