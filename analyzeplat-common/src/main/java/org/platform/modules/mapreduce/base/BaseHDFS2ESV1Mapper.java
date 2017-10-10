package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.platform.utils.json.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseHDFS2ESV1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2ESV1Mapper.class);
	
	private TransportClient client = null;
	
	private String esIndex = null;
	
	private String esType = null;
	
	private int batchSize = 1000;
	
	private List<Map<String, Object>> records = null;
	
	private Map<String, Text> mapRecords = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.esIndex = (String) context.getConfiguration().get("esIndex");
		this.esType = (String) context.getConfiguration().get("esType");
		this.batchSize = Integer.parseInt(String.valueOf(context.getConfiguration().get("batchSize", "1000")));
		this.records = new ArrayList<Map<String, Object>>(this.batchSize);
		this.mapRecords = new HashMap<String, Text>();
		String esClusterName = (String) context.getConfiguration().get("esClusterName");
		String esClusterIP = (String) context.getConfiguration().get("esClusterIP");
		Settings settings = Settings.builder().put("cluster.name", esClusterName)
				.put("client.tansport.sniff", true).build();
		client = TransportClient.builder().settings(settings).build();
		List<EsServerAddress> serverAddress = new ArrayList<EsServerAddress>();
		String[] esClusterIPs = esClusterIP.contains(",") ? 
				esClusterIP.split(",") : new String[]{esClusterIP};
		for (int i = 0, len = esClusterIPs.length; i < len; i++) {
			serverAddress.add(new EsServerAddress(esClusterIPs[i], 9030));
		}
		for (EsServerAddress address : serverAddress) {
			client.addTransportAddress(new InetSocketTransportAddress(
					new InetSocketAddress(address.getHost(), address.getPort())));
		}
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Map<String, Object> record = GsonUtils.fromJsonToMap(value.toString());
			handle(record);
			records.add(record);
			mapRecords.put((String) record.get("_id"), new Text(GsonUtils.fromMapToJson(record)));
			if (records.size() > this.batchSize) {
				List<String> notExistIds = bulkInsertIndexTypeDatas(records);
				for (int i = 0, len = notExistIds.size(); i < len; i++) {
					context.write(new Text(notExistIds.get(i)), mapRecords.get(notExistIds.get(i)));
				}
				records.clear();
				mapRecords.clear();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	protected void handle(Map<String, Object> original) {
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
		if (!records.isEmpty()) {
			List<String> notExistIds = bulkInsertIndexTypeDatas(records);
			for (int i = 0, len = notExistIds.size(); i < len; i++) {
				context.write(new Text(notExistIds.get(i)), mapRecords.get(notExistIds.get(i)));
			}
			records.clear();
			mapRecords.clear();
		}
		client.close();
	}
	
	/**
	 * 批量插入ES
	 * @param datas
	 */
	private List<String> bulkInsertIndexTypeDatas(List<Map<String, Object>> datas) {
		if (null == datas || datas.isEmpty()) return new ArrayList<String>();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = datas.size(); i < len; i++) {
			source = datas.get(i);
			if (source.containsKey("_id")) {
				String _id = (String) source.remove("_id");
				irb = client.prepareIndex(this.esIndex, this.esType, _id);
				irb.setSource(source);
			} else {
				irb = client.prepareIndex(this.esIndex, this.esType).setSource(source);
			}
			bulkRequestBuilder.add(irb);
		}
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			LOG.info(bulkResponse.buildFailureMessage());
		}
		List<String> notExistIds = new ArrayList<String>();
		BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
		for (int i = 0, len = bulkItemResponses.length; i < len; i++) {
			BulkItemResponse bulkItemResponse = bulkItemResponses[i];
			long version = bulkItemResponse.getVersion();
			if (version == 1) {
				notExistIds.add(bulkItemResponse.getId());
			}
		}
		return notExistIds;
	}

}

class EsServerAddress implements Serializable {

	private static final long serialVersionUID = 1L;

	private String host = null;
	private Integer port = 9300;

	public EsServerAddress() {
		super();
	}

	public EsServerAddress(String host) {
		super();
		this.host = host;
	}

	public EsServerAddress(String host, Integer port) {
		super();
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((port == null) ? 0 : port.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EsServerAddress other = (EsServerAddress) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port == null) {
			if (other.port != null)
				return false;
		} else if (!port.equals(other.port))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("EsServerAddress [host=");
		builder.append(host);
		builder.append(", port=");
		builder.append(port);
		builder.append("]");
		return builder.toString();
	}

}
