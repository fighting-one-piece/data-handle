package org.platform.utils.json;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class MapDeserializer implements JsonDeserializer<Map<String, Object>> {

	/**
	 * 默认会将Object接收的数字转换为double 
	 * int类型12转换后会变成12.0，自定义转换的目的就是将他转换为12
	 */
	public Map<String, Object> deserialize(JsonElement json, Type type, JsonDeserializationContext context)
			throws JsonParseException {
		Map<String, Object> map = new HashMap<String, Object>();
		Set<Entry<String, JsonElement>> entries = json.getAsJsonObject().entrySet();
		for (Entry<String, JsonElement> entry : entries) {
			JsonElement element = entry.getValue();
			if (null == element || element instanceof JsonNull) continue;
			Class<?> elementClass = element.getClass();
			if (JsonArray.class.isAssignableFrom(elementClass)) {
				map.put(entry.getKey(), GsonUtils.builder().toJson(element));
			} else if (JsonObject.class.isAssignableFrom(elementClass)) {
				map.put(entry.getKey(), GsonUtils.builder().toJson(element));
			} else {
				map.put(entry.getKey(), isNumberic(element.getAsString()) ? element.getAsLong() : element.getAsString());
			}
		}
		return map;
	}
	
	private static final String REG_1 = "0[0-9]{9,11}";
	private static final String REG_2 = "\\d{1,18}|[0-8][0-9]{18}|9[0-1]\\d{17}";

	/**
	 * 判断是不是数字
	 * @param input
	 * @return 是数字类型返回true
	 */
	public boolean isNumberic(String input) {
		return Pattern.compile(REG_1).matcher(input).matches() ? false : 
			Pattern.compile(REG_2).matcher(input).matches() ? true : false;
	}

}
