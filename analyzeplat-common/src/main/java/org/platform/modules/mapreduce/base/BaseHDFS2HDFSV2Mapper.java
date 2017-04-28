package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 主要处理数据格式为文本的输入文件
 */
public abstract class BaseHDFS2HDFSV2Mapper extends BaseHDFS2HDFSMapper {
	
	/** 分隔符*/
	private static final String DELIMITER = "\\$#\\$";
	/** 列名称*/
	private String[] metadatas = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		String column_metadata = context.getConfiguration().get("column.metadata");
		if (null == column_metadata || "".equals(column_metadata)) {
			throw new RuntimeException("列源数据不能为空");
		}
		metadatas = column_metadata.split(DELIMITER);
	}
	
	@Override
	public Map<String, Object> extractInputRecord(String inputRecord) {
		Map<String, Object> result = new HashMap<String, Object>();
		String[] fields = inputRecord.split(DELIMITER);
		for (int i = 0, len = metadatas.length; i < len; i++) {
			result.put(metadatas[i], fields[i]);
		}
		return result;
	}

}
