package org.platform.modules.mapreduce.clean.ljp.HDFS2HDFS.V2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.platform.utils.MD5Utils;

public class LogisticsIDGenerator {

	public static String generateByMapValues(Map<String, Object> map, String... excludeKeys) {
		List<String> excludeKeyList = Arrays.asList(excludeKeys);
		StringBuilder sb = new StringBuilder(100);
		for (Map.Entry<String, Object> entry: map.entrySet()) {
			if (excludeKeyList.contains(entry.getKey())) continue;
			Object value = entry.getValue();
			if (null == value) continue;
			if (value instanceof String) value = String.valueOf(value).trim();
			sb.append(value);
		}
		return MD5Utils.hash(sb.toString());
	}
	
}
