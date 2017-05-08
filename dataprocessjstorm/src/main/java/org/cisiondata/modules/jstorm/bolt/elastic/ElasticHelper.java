package org.cisiondata.modules.jstorm.bolt.elastic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cisiondata.modules.elastic5.ESClient;
import org.cisiondata.utils.json.GsonUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class ElasticHelper {
	
	private static Logger LOG = LoggerFactory.getLogger(ElasticHelper.class);

	private List<Tuple> tuples = null;
	
	private List<String> datas = null;
	
	private int batchSize = 1000;
	
	private OutputCollector collector = null;
	
	public ElasticHelper(int batchSize, OutputCollector collector) {
		this.tuples = new ArrayList<Tuple>();
		this.datas = new ArrayList<String>();
		if (batchSize > 0) this.batchSize = batchSize;
		this.collector = collector;
	}
	
	public void add(Tuple tuple) {
		System.out.println("elastic: " + tuple.getString(0));
		tuples.add(tuple);
		datas.add(tuple.getString(0));
		if (tuples.size() == batchSize) {
			bulkInsert(datas);
			datas.clear();
			ack();
		}
	}
	
	public void ack() {
		for (int i = 0, len = tuples.size(); i < len; i++) {
			collector.ack(tuples.get(i));
		}
		tuples.clear();
	}
	
	public void fail(Exception e) {
		collector.reportError(e);
		for (int i = 0, len = tuples.size(); i < len; i++) {
			collector.fail(tuples.get(i));
		}
		tuples.clear();
		datas.clear();
	}
	
	public static void bulkInsert(List<String> datas) {
		if (null == datas || datas.size() == 0) return;
		Client client = ESClient.getInstance().getClient();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		try {
			IndexRequestBuilder irb = null;
			Map<String, Object> source = null;
			for (int i = 0, len = datas.size(); i < len; i++) {
				source = GsonUtils.fromJsonToMap(datas.get(i));
				String index = String.valueOf(source.remove("index"));
				String type = String.valueOf(source.remove("type"));
				if (source.containsKey("_id")) {
					String _id = (String) source.remove("_id");
					irb = client.prepareIndex(index, type, _id).setSource(source);
				} else {
					irb = client.prepareIndex(index, type).setSource(source);
				}
				bulkRequestBuilder.add(irb);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			LOG.info(bulkResponse.buildFailureMessage());
		}
	}
	
}
