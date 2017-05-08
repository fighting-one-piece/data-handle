package org.cisiondata.modules.elastic.service;

import java.util.List;
import java.util.Map;

import org.cisiondata.modules.abstr.entity.QueryResult;
import org.elasticsearch.index.query.QueryBuilder;

public interface IElastic5Service {

	/**
	 * 读取数据列表
	 * @param index
	 * @param type
	 * @param query
	 * @param size
	 * @param isHighLight
	 * @return
	 */
	public List<Map<String, Object>> readDataList(String index, String type, 
			QueryBuilder query, int size, boolean isHighLight);
	
	
	/**
	 * 读取分页数据列表
	 * @param index
	 * @param type
	 * @param query
	 * @param scrollId
	 * @param size
	 * @param isHighLight
	 * @return
	 */
	public QueryResult<Map<String, Object>> readPaginationDataList(String index, String type, 
			QueryBuilder query, String scrollId, int size, boolean isHighLight);
	
}
