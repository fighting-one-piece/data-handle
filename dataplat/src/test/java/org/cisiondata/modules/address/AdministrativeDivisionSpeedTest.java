package org.cisiondata.modules.address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cisiondata.modules.address.service.impl.AddressServiceImpl;
import org.cisiondata.utils.file.DefaultLineHandler;
import org.cisiondata.utils.file.FileUtils;
import org.cisiondata.utils.http.HttpUtils;
import org.cisiondata.utils.json.GsonUtils;

public class AdministrativeDivisionSpeedTest {

	public static void speed() {
		try {
			AddressServiceImpl service = new AddressServiceImpl();
			service.initializing();
			List<String> lines = FileUtils.readFromAbsolute("F:\\document\\doc\\201704\\address100000.txt", new DefaultLineHandler());
			long startTime = System.currentTimeMillis();
			for (int i = 0, len = lines.size(); i < len; i++) {
				Map<String, Object> map = GsonUtils.fromJsonToMap(lines.get(i));
				List<String> words = service.read3AdministrativeDivision(String.valueOf(map.get("address")));
				System.out.println(words);
			}
			System.out.println(lines.size() + " spend time: " + (System.currentTimeMillis() - startTime) / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void speed01() {
		try {
			List<String> lines = FileUtils.readFromAbsolute("F:\\document\\doc\\201704\\address100000.txt", new DefaultLineHandler());
			long startTime = System.currentTimeMillis();
			for (int i = 0, len = lines.size(); i < len; i++) {
				Map<String, Object> map = GsonUtils.fromJsonToMap(lines.get(i));
				extract3ADFromAddress(String.valueOf(map.get("address")));
//				List<String> words = extract3ADFromAddress(String.valueOf(map.get("address")));
//				System.out.println(words);
			}
			System.out.println(lines.size() + " spend time: " + (System.currentTimeMillis() - startTime) / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public static List<String> extract3ADFromAddress(String address) {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("address", address);
		String json = HttpUtils.sendGet("http://localhost:10000/ads", params);
//		String json = HttpUtils.sendGet("http://192.168.0.15:10000/ads", params);
//		String json = HttpUtils.sendGet("http://192.168.0.114:8060/dataplat/ads", params);
		System.out.println("json: " + json);
		Map<String, Object> map = GsonUtils.fromJsonToMap(json);
		if (null == map) return new ArrayList<String>();
		Object data = map.get("data");
		return null == data ? new ArrayList<String>() : GsonUtils.builder().fromJson((String) data, List.class);
	}
	
	public static void main(String[] args) {
//		speed01();
		System.out.println(extract3ADFromAddress("澄迈"));
	}
	
}
