package org.platform.utils;

import java.util.HashMap;
import java.util.Map;

import org.platform.utils.http.HttpUtils;

public class ElasticUtils {

	public static String bulk(String index, String type, String source) {
		String url = String.format("http://192.168.0.15:10020/elastic5/index/%s/type/%s/bulk", index, type);
		Map<String, String> params = new HashMap<String, String>();
		params.put("source", source);
		return HttpUtils.sendPost(url, params);
	}
	
}
