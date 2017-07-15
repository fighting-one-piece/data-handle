package org.cisiondata.modules.elastic5;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESClient {
	
	private TransportClient client = null;
	
	private ESClient() {
		initClient();
	}
	
	private static class ESClientHolder {
		private static final ESClient INSTANCE = new ESClient();
	}
	
	public static final ESClient getInstance() {
		return ESClientHolder.INSTANCE;
	}
	
	public Client getClient() {
		if (null == client) initClient();
		return client;
	}
	
	public void closeClient(Client client) {
		client.close();
	}
	
	private void initClient() {
        Settings settings = Settings.builder().put("cluster.name", "cisiondata-cluster")
        		.put("client.transport.sniff", true).build();
        client = new PreBuiltTransportClient(settings);
        List<EsServerAddress> esServerAddress = new ArrayList<EsServerAddress>();
		esServerAddress.add(new EsServerAddress("172.20.100.15", 9030));
		esServerAddress.add(new EsServerAddress("172.20.100.16", 9030));
		esServerAddress.add(new EsServerAddress("172.20.100.17", 9030));
		for (EsServerAddress address : esServerAddress) {
			client.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}
	
}

