package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cisiondata.modules.jstorm.spout.kafka.DynamicBrokersReader;
import org.cisiondata.modules.jstorm.spout.kafka.ZkHosts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBrokerReader implements IBrokerReader {

	private static final Logger LOG = LoggerFactory.getLogger(ZkBrokerReader.class);

	List<GlobalPartitionInformation> cachedBrokers = new ArrayList<GlobalPartitionInformation>();
	DynamicBrokersReader reader;
	long lastRefreshTimeMs;

	long refreshMillis;

	@SuppressWarnings("rawtypes")
	public ZkBrokerReader(Map conf, String topic, ZkHosts hosts) {
		try {
			reader = new DynamicBrokersReader(conf, hosts.brokerZkStr, hosts.brokerZkPath, topic);
			cachedBrokers = reader.getBrokerInfo();
			lastRefreshTimeMs = System.currentTimeMillis();
			refreshMillis = hosts.refreshFreqSecs * 1000L;
		} catch (java.net.SocketTimeoutException e) {
			LOG.warn("Failed to update brokers", e);
		}

	}

	private void refresh() {
		long currTime = System.currentTimeMillis();
		if (currTime > lastRefreshTimeMs + refreshMillis) {
			try {
				LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
				cachedBrokers = reader.getBrokerInfo();
				lastRefreshTimeMs = currTime;
			} catch (java.net.SocketTimeoutException e) {
				LOG.warn("Failed to update brokers", e);
			}
		}
	}
	@Override
	public GlobalPartitionInformation getBrokerForTopic(String topic) {
		refresh();
        for(GlobalPartitionInformation partitionInformation : cachedBrokers) {
            if (partitionInformation.topic.equals(topic)) return partitionInformation;
        }
		return null;
	}

	@Override
	public List<GlobalPartitionInformation> getAllBrokers() {
		refresh();
		return cachedBrokers;
	}

	@Override
	public void close() {
		reader.close();
	}
}
