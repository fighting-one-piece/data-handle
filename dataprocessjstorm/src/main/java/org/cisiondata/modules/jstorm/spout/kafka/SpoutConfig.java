package org.cisiondata.modules.jstorm.spout.kafka;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public class SpoutConfig extends KafkaConfig implements Serializable {
    public List<String> zkServers = null;
    public Integer zkPort = null;
    public String zkRoot = null;
    public String id = null;

    public String outputStreamId;

    // setting for how often to save the current kafka offset to ZooKeeper
    public long stateUpdateIntervalMs = 2000;

    // Retry strategy for failed messages
    public String failedMsgRetryManagerClass = ExponentialBackoffMsgRetryManager.class.getName();

    // Exponential back-off retry settings.  These are used by ExponentialBackoffMsgRetryManager for retrying messages after a bolt
    // calls OutputCollector.fail(). These come into effect only if ExponentialBackoffMsgRetryManager is being used.
    public long retryInitialDelayMs = 0;
    public double retryDelayMultiplier = 1.0;
    public long retryDelayMaxMs = 60 * 1000;
    public int retryLimit = -1;

    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
