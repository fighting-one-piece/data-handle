package org.cisiondata.modules.jstorm.spout.kafka.trident;

import org.cisiondata.modules.jstorm.spout.kafka.BrokerHosts;
import org.cisiondata.modules.jstorm.spout.kafka.KafkaConfig;

@SuppressWarnings("serial")
public class TridentKafkaConfig extends KafkaConfig {

    public final IBatchCoordinator coordinator = new DefaultCoordinator();

    public TridentKafkaConfig(BrokerHosts hosts, String topic) {
        super(hosts, topic);
    }

    public TridentKafkaConfig(BrokerHosts hosts, String topic, String clientId) {
        super(hosts, topic, clientId);
    }

}
