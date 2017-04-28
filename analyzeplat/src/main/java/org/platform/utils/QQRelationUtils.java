package org.platform.utils;

import java.util.HashMap;
import java.util.Map;

import org.platform.utils.http.HttpUtils;

public class QQRelationUtils {
	
	public static String insertQQNode(String nodeJSON) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("nodeJSON", nodeJSON);
		return HttpUtils.sendPost("http://192.168.0.15:10010/qqs", params);
	}
	
	public static String insertQunNode(String nodeJSON) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("nodeJSON", nodeJSON);
		return HttpUtils.sendPost("http://192.168.0.15:10010/quns", params);
	}
	
	public static String insertRelation(String nodeJSON) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("nodeJSON", nodeJSON);
		return HttpUtils.sendPost("http://192.168.0.15:10010/relations", params);
	}
	
}
