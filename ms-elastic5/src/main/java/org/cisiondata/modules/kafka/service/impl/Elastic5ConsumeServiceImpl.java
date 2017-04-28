package org.cisiondata.modules.kafka.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cisiondata.modules.elastic.utils.ESClient;
import org.cisiondata.modules.kafka.CommonConsumer;
import org.cisiondata.modules.kafka.service.IConsumeService;
import org.cisiondata.utils.json.GsonUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service("elastic5ConsumeService")
public class Elastic5ConsumeServiceImpl implements IConsumeService {
	
	private Logger LOG = LoggerFactory.getLogger(Elastic5ConsumeServiceImpl.class);
	
	private static List<Map<String, Object>> queue = new ArrayList<Map<String, Object>>();
	
	private static int BATCH = 1000;
	
	@Override
	public void handle(String message) throws RuntimeException {
		queue.add(GsonUtils.fromJsonToMap(message));
		if (queue.size() == BATCH) {
			bulkInsertES5(queue);
			queue.clear();
		}
	}
	
	private void bulkInsertES5(List<Map<String, Object>> queue) {
		if (null == queue || queue.isEmpty()) return;
		Client client = ESClient.getInstance().getClient();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = queue.size(); i < len; i++) {
			source = queue.get(i);
			String index = String.valueOf(source.remove("index"));
			String type = String.valueOf(source.remove("type"));
			if (source.containsKey("_id")) {
				String _id = (String) source.remove("_id");
				irb = client.prepareIndex(index, type, _id);
				irb.setSource(source);
			} else {
				irb = client.prepareIndex(index, type).setSource(source);
			}
			bulkRequestBuilder.add(irb);
		}
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			LOG.info(bulkResponse.buildFailureMessage());
			System.out.println(bulkResponse.buildFailureMessage());
		}
	}
	
	public static void main(String[] args) {
		new CommonConsumer("elastic5", new Elastic5ConsumeServiceImpl()).start();
	}
	
}
