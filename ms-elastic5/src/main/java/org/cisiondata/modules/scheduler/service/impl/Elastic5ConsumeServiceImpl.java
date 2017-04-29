package org.cisiondata.modules.scheduler.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.cisiondata.modules.elastic.utils.ESClient;
import org.cisiondata.modules.scheduler.service.IConsumeService;
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
	
	@Override
	public void handle(String message) throws RuntimeException {
		if (StringUtils.isBlank(message)) return;
		Map<String, Object> source = GsonUtils.fromJsonToMap(message);
		Client client = ESClient.getInstance().getClient();
		IndexRequestBuilder irb = null;
		String index = String.valueOf(source.remove("index"));
		String type = String.valueOf(source.remove("type"));
		if (source.containsKey("_id")) {
			String _id = (String) source.remove("_id");
			irb = client.prepareIndex(index, type, _id).setSource(source);
		} else {
			irb = client.prepareIndex(index, type).setSource(source);
		}
		try {
			irb.execute().get();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	@Override
	public void handle(List<String> messages) throws RuntimeException {
		if (null == messages || messages.size() == 0) return;
		Client client = ESClient.getInstance().getClient();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		try {
			IndexRequestBuilder irb = null;
			Map<String, Object> source = null;
			for (int i = 0, len = messages.size(); i < len; i++) {
				source = GsonUtils.fromJsonToMap(messages.get(i));
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
			System.out.println(bulkResponse.buildFailureMessage());
		}
	}
	
}
