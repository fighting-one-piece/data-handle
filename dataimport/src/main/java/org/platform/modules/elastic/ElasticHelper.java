package org.platform.modules.elastic;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class ElasticHelper {
	
	public static void deleteIndexTypeDatas(String index, String type) {
		Client client = ElasticClient.getInstance().getClient();
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery()).setSearchType(SearchType.QUERY_THEN_FETCH)
					.setScroll(TimeValue.timeValueMinutes(10)).setSize(10000).setExplain(false).execute().actionGet();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (true) {
			SearchHit[] hitArray = response.getHits().getHits();
			SearchHit hit = null;
			for (int i = 0, len = hitArray.length; i < len; i++) {
				hit = hitArray[i];
				DeleteRequestBuilder request = client.prepareDelete(index, type, hit.getId());
				bulkRequest.add(request);
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				System.err.println(bulkResponse.buildFailureMessage());
			}
			if (hitArray.length == 0) break;
			response = client.prepareSearchScroll(response.getScrollId())
							.setScroll(TimeValue.timeValueMinutes(10)).execute().actionGet();
		}
		ElasticClient.getInstance().closeClient(client);
	}
	
}
