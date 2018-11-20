package com.alibaba.datax.plugin.reader.esreader;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.google.gson.Gson;

public class ESReader extends Reader {

	public static class Job extends Reader.Job {
		
		private Configuration originalConfiguration = null;
		
		@Override
		public void preCheck() {
			super.preCheck();
		}

		@Override
		public void preHandler(Configuration jobConfiguration) {
			super.preHandler(jobConfiguration);
		}
		
		@Override
		public void init() {
			this.originalConfiguration = super.getPluginJobConf();
		}
		
		@Override
		public void prepare() {
			super.prepare();
		}

		@Override
		public void post() {
			super.post();
		}
		
		@Override
		public void postHandler(Configuration jobConfiguration) {
			super.postHandler(jobConfiguration);
		}
		
		@Override
		public void destroy() {
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> readerSplitConfigurations = new ArrayList<Configuration>();
			for (int i = 0; i < adviceNumber; i++) {
				Configuration readerSplitConfiguration = this.originalConfiguration.clone();
				readerSplitConfigurations.add(readerSplitConfiguration);
			}
			return readerSplitConfigurations;
		}
		
	}
	
	public static class Task extends Reader.Task {
		
		private Configuration readerSplitConfiguration = null;
		
		private String esClusterName = null;
		
		private String esClusterIP = null;

		private Integer esClusterPort = null;
		
		private String esIndex = null;
		
		private String esType = null;
		
		private String esFieldInclude = null;
		
		private String[] esFields = null;
		
		private Gson gson = null;
		
		private TransportClient client = null;
		
		private Integer batchSize = null;
		
		private Long readSize = null;
		
		@Override
		public void preCheck() {
			super.preCheck();
		}
		
		@Override
		public void preHandler(Configuration jobConfiguration) {
			super.preHandler(jobConfiguration);
		}
		
		@Override
		public void init() {
			this.readerSplitConfiguration = super.getPluginJobConf();
			this.esClusterName = readerSplitConfiguration.getString(Key.esClusterName);
			this.esClusterIP = readerSplitConfiguration.getString(Key.esClusterIP);
			this.esClusterPort = readerSplitConfiguration.getInt(Key.esClusterPort, 9300);
			this.esIndex = readerSplitConfiguration.getString(Key.esIndex);
			this.esType = readerSplitConfiguration.getString(Key.esType);
			this.esFieldInclude = readerSplitConfiguration.getString(Key.esFieldInclude);
			this.batchSize = readerSplitConfiguration.getInt(Key.batchSize, 1000);
			this.readSize = readerSplitConfiguration.getLong(Key.readSize, Long.MAX_VALUE);
			this.gson = new Gson();
		}
		
		@Override
		public void prepare() {
			super.prepare();
			Settings settings = Settings.builder().put("cluster.name", esClusterName)
					.put("client.transport.sniff", true).build();
			client = TransportClient.builder().settings(settings).build();
			List<EsServerAddress> serverAddress = new ArrayList<EsServerAddress>();
			String[] esClusterIPs = esClusterIP.contains(",") ? 
					esClusterIP.split(",") : new String[]{esClusterIP};
			for (int i = 0, len = esClusterIPs.length; i < len; i++) {
				serverAddress.add(new EsServerAddress(esClusterIPs[i], esClusterPort));
			}
			for (EsServerAddress address : serverAddress) {
				client.addTransportAddress(new InetSocketTransportAddress(
						new InetSocketAddress(address.getHost(), address.getPort())));
			}
			if (null != esFieldInclude) {
				esFields = esFieldInclude.contains(",") ? 
						esFieldInclude.split(",") : new String[]{esFieldInclude};
			}
		}
		
		@Override
		public void post() {
			super.post();
		}
		
		@Override
		public void postHandler(Configuration jobConfiguration) {
			super.postHandler(jobConfiguration);
		}

		@Override
		public void destroy() {
			client.close();
		}
		
		@Override
		public void startRead(RecordSender recordSender) {
			SearchResponse response = client.prepareSearch(esIndex).setTypes(esType)
					.setQuery(QueryBuilders.matchAllQuery()).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setScroll(new TimeValue(60000)).setSize(batchSize).setExplain(true).execute().actionGet();
			Record record = null;
			Map<String, Object> source = null;
			Map<String, Object> targetSource = null;
			String key = null;
			Object value = null;
			boolean esFieldsExist = null == esFields || esFields.length == 0 ? false : true;
			long totalHits = 0L;
			while (true) {
				SearchHit[] hitArray = response.getHits().getHits();
				for (int i = 0, len = hitArray.length; i < len; i++) {
					record = recordSender.createRecord();
					source = hitArray[i].getSource();
					if (esFieldsExist) {
						targetSource = new HashMap<String, Object>();
						for (int j = 0, jLen = esFields.length; j < jLen; j++) {
							key = esFields[j];
							if (source.containsKey(key)) {
								value = source.get(key);
								value = (null == value || "" == value) ? "NA" : value;
							} else {
								value = "NA";
							}
							targetSource.put(key, value);
						}
						record.addColumn(new StringColumn(gson.toJson(targetSource)));
					} else {
						record.addColumn(new StringColumn(gson.toJson(source)));
					}
					recordSender.sendToWriter(record);
				}
				totalHits += hitArray.length;
				if (hitArray.length == 0 || totalHits >= readSize) break;
				response = client.prepareSearchScroll(response.getScrollId())
								.setScroll(new TimeValue(60000)).execute().actionGet();
			}
		}
		
	}
	
}
