package org.cisiondata.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESClient {

	private TransportClient client20 = null;
	private TransportClient client125 = null;
	
	private ESClient() {
		initClient125();
	}

	private static class ESClientHolder {
		private static final ESClient INSTANCE = new ESClient();
	}

	public static final ESClient getInstance() {
		return ESClientHolder.INSTANCE;
	}

	public Client getClient() {
		return getClient125();
	}

	public Client getClient20() {
		if (null == client20)
			initClient20();
		return client20;
	}
	
	public Client getClient125() {
		if (null == client125)
			initClient125();
		return client125;
	}
	
	public void closeClient(Client client) {
		client.close();
	}
	private void initClient20() {
		Settings testSettings = Settings.builder().put("cluster.name", "cisiondata")
				.put("client.transport.sniff", true).build();
		client20 = TransportClient.builder().settings(testSettings).build();
		List<EsServerAddress> testServerAddress = new ArrayList<EsServerAddress>();
		testServerAddress.add(new EsServerAddress("192.168.0.20", 9030));
		for (EsServerAddress address : testServerAddress) {
			client20.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}
	private void initClient125() {
		Settings testSettings = Settings.builder().put("cluster.name", "cisiondata")
				.put("client.transport.sniff", true).build();
		client125 = TransportClient.builder().settings(testSettings).build();
		List<EsServerAddress> testServerAddress = new ArrayList<EsServerAddress>();
		testServerAddress.add(new EsServerAddress("192.168.0.125", 9030));
		for (EsServerAddress address : testServerAddress) {
			client125.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}
}
