package org.cisiondata.modules.elastic.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.cisiondata.modules.elastic.service.IElasticService;
import org.cisiondata.modules.elastic.utils.ESClient;
import org.cisiondata.utils.json.GsonUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service("elasticService")
public class ElasticServiceImpl implements IElasticService {
	
	private Logger LOG = LoggerFactory.getLogger(ElasticServiceImpl.class);

	private static Map<String, List<Map<String, Object>>> queue = new HashMap<String, List<Map<String, Object>>>();
	
	private static int BATCH = 1000;
	
	@Override
	public void bulkInsert(String index, String type, String source) throws RuntimeException {
		if (StringUtils.isBlank(index) || StringUtils.isBlank(type) || StringUtils.isBlank(source)) {
			throw new RuntimeException("index and type and source must be not null");
		}
		String key = index + ":" + type;
		List<Map<String, Object>> datas = null;
		if (!queue.containsKey(key)) {
			datas = new ArrayList<Map<String, Object>>();
			queue.put(key, datas);
		} else {
			datas = queue.get(key);
		}
		datas.add(GsonUtils.fromJsonToMap(source));
		if (datas.size() == BATCH) {
			bulkInsertIndexTypeDatas(index, type, datas);
			queue.remove(key);
		}
	}
	
	/**
	 * 批量插入ES
	 * @param datas
	 */
	private void bulkInsertIndexTypeDatas(String index, String type, List<Map<String, Object>> datas) {
		if (null == datas || datas.isEmpty()) return;
		Client client = ESClient.getInstance().getClient();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = datas.size(); i < len; i++) {
			source = datas.get(i);
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
		}
	}
	
}
