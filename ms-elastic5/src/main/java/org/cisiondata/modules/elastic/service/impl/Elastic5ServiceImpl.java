package org.cisiondata.modules.elastic.service.impl;

import java.util.List;
import java.util.Map;

import org.cisiondata.modules.abstr.entity.QueryResult;
import org.cisiondata.modules.elastic.service.IElastic5Service;
import org.elasticsearch.index.query.QueryBuilder;
import org.springframework.stereotype.Service;

@Service("elastic5Service")
public class Elastic5ServiceImpl implements IElastic5Service {

	@Override
	public List<Map<String, Object>> readDataList(String index, String type, QueryBuilder query, int size,
			boolean isHighLight) {
		return null;
	}

	@Override
	public QueryResult<Map<String, Object>> readPaginationDataList(String index, String type, QueryBuilder query,
			String scrollId, int size, boolean isHighLight) {
		return null;
	}
	
	
	
}
