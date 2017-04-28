package org.platform.modules.mapreduce.base;

import java.util.Map;

/**
 * 主要处理数据格式为JSON的输入文件
 */
public abstract class BaseHDFS2HDFSV1Mapper extends BaseHDFS2HDFSMapper {
	
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> extractInputRecord(String inputRecord) {
		Map<String, Object> original = null;
		try {
			original = gson.fromJson(inputRecord, Map.class);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return original;
	}
	
}
