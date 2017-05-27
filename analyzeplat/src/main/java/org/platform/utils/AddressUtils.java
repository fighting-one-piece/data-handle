package org.platform.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.platform.utils.http.HttpClientUtils;
import org.platform.utils.json.GsonUtils;

public class AddressUtils {
	
	@SuppressWarnings("unchecked")
	public static List<String> extractADFromAddress(String address) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("address", address);
		String json = HttpClientUtils.get("http://192.168.0.115:18000/ads", params, "UTF-8");
		Map<String, Object> map = GsonUtils.fromJsonToMap(json);
		Object data = map.get("data");
		return null == data ? new ArrayList<String>() : GsonUtils.builder().fromJson((String) data, List.class);
	}
	
	@SuppressWarnings("unchecked")
	public static List<String> extract3ADFromAddress(String address) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("address", address);
		String json = HttpClientUtils.get("http://192.168.0.115:18000/3/ads", params, "UTF-8");
		Map<String, Object> map = GsonUtils.fromJsonToMap(json);
		Object data = map.get("data");
		return null == data ? new ArrayList<String>() : GsonUtils.builder().fromJson((String) data, List.class);
	}

	@SuppressWarnings("unchecked")
	public static List<String> extract5ADFromAddress(String address) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("address", address);
		String json = HttpClientUtils.get("http://192.168.0.115:18000/5/ads", params, "UTF-8");
		Map<String, Object> map = GsonUtils.fromJsonToMap(json);
		Object data = map.get("data");
		return null == data ? new ArrayList<String>() : GsonUtils.builder().fromJson((String) data, List.class);
	}
	
	public static void main(String[] args) {
		System.out.println(extractADFromAddress("武城县老车站南张庄新区2－1－402"));
		System.out.println(extract3ADFromAddress("武城县老车站南张庄新区2－1－402"));
		System.out.println(extract5ADFromAddress("武城县老车站南张庄新区2－1－402"));
	}
	
}
