package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.platform.utils.json.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseHDFS2ESV2Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2ESV2Mapper.class);
	
	private TransportClient client = null;
	
	private String esIndex = null;
	
	private String esType = null;
	
	private int batchSize = 1000;
	
	private List<Map<String, Object>> records = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.esIndex = (String) context.getConfiguration().get("esIndex");
		this.esType = (String) context.getConfiguration().get("esType");
		this.batchSize = Integer.parseInt(String.valueOf(context.getConfiguration().get("batchSize", "1000")));
		this.records = new ArrayList<Map<String, Object>>(this.batchSize);
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
			if (records.size() > this.batchSize) {
				bulkInsertIndexTypeDatas(records);
				records.clear();
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
			bulkInsertIndexTypeDatas(records);
			records.clear();
		}
		client.close();
	}
	
	/**
	 * 批量插入ES
	 * @param datas
	 */
	private void bulkInsertIndexTypeDatas(List<Map<String, Object>> datas) {
		if (null == datas || datas.isEmpty()) return;
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
	}

}

