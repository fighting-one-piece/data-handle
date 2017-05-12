package org.cisiondata.modules.elastic.service.impl;

import java.util.List;
import java.util.Map;

import org.cisiondata.modules.abstr.entity.QueryResult;
import org.cisiondata.modules.elastic.entity.SearchParams;
import org.cisiondata.modules.elastic.service.IElastic5Service;
import org.cisiondata.modules.elastic.utils.Elastic5Client;
import org.cisiondata.utils.exception.BusinessException;
import org.cisiondata.utils.param.ParamsUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Service;

@Service("elastic5Service")
public class Elastic5ServiceImpl implements IElastic5Service {
	
	@Override
	public Object readDataList(String indices, String types, String fields, String keywords, int highLight,
			Integer currentPageNum, Integer rowNumPerPage) throws BusinessException {
		ParamsUtils.checkNotNull(indices, "indices is null");
		ParamsUtils.checkNotNull(types, "types is null");
		ParamsUtils.checkNotNull(keywords, "keywords is null");
		SearchParams params = new SearchParams(indices, types, fields, keywords, highLight, currentPageNum, rowNumPerPage);
		
		SearchRequestBuilder searchRequestBuilder = Elastic5Client.getInstance().getClient()
				.prepareSearch(params.indices()).setTypes(params.types());
		searchRequestBuilder.setQuery(null);
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setSize(100);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		SearchHit[] hitArray = response.getHits().getHits();
		for (int i = 0, len = hitArray.length; i < len; i++) {
			
		}
		return null;
	}

	@Override
	public List<Map<String, Object>> readDataList(String index, String type, QueryBuilder query, int size,
			boolean isHighLight) throws BusinessException {
		return null;
	}

	@Override
	public QueryResult<Map<String, Object>> readPaginationDataList(String index, String type, QueryBuilder query,
			String scrollId, int size, boolean isHighLight) throws BusinessException {
		return null;
	}

	
	
}
