package org.platform.modules.mapreduce.clean.ly.cleanTool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BocaiTool {
	// 存在主要字段，但是不满足条件的value值
	static List<String> erroList = new ArrayList<String>();
	static {
		erroList.add("NA");
		erroList.add("NULL");
		erroList.add("");
		erroList.add("null");
	}
	// 跳过的特殊字段
	static List<String> limitList = new ArrayList<String>();
	static {
		limitList.add("updateTime");
		limitList.add("_id");
	}

	/**
	 * 寻找指定字段的匹配值
	 * 
	 * @param original
	 *            原集合
	 * @param regex
	 *            需要找的字段的正则表达式
	 * @return
	 */
	public static String findValue(Map<String, Object> original, String regex) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		Iterator<Map.Entry<String, Object>> maps = map.entrySet().iterator();
		while (maps.hasNext()) {
			Entry<String, Object> entry = maps.next();
			// 获取当前遍历的key和value值
			String key = entry.getKey();
			String value = (String) entry.getValue();
			// 特殊字段跳过
			if (limitList.contains(key))
				continue;
			// 找到匹配的值
			if (value != null) {
				String finalValue = cleanFile(original, regex, key, value);
				if (finalValue != null) {
					return finalValue;
				}
			}
		}
		return null;
	}

	/**
	 * @param regex
	 *            正则表达式
	 * @param fileValue
	 *            字段值
	 * @return
	 */
	public static String cleanFile(Map<String, Object> original, String regex, String fileName, String fileValue) {
		Pattern pat = Pattern.compile(regex);
		Matcher M = pat.matcher(fileValue);
		StringBuilder sb = new StringBuilder();
		String lastValue = null;
		while (M.find()) {
			if (lastValue != null) {
				lastValue = lastValue.replace(M.group(), "");
			} else {
				lastValue = fileValue.replace(M.group(), "");
			}
			sb.append(M.group() + ",");
		}
		if (sb.length() != 0) {
			original.put(fileName, lastValue);
			return sb.deleteCharAt(sb.lastIndexOf(",")).toString();
		} else {
			return null;
		}
	}

	/**
	 * 根据传入的值，判断是否加入cnote
	 */
	public static void addCnote(Map<String, Object> original, String value, String fileName) {
		if (value != null && !erroList.contains(value)) {
			if (original.containsKey("cnote")) {
				String cnote = (String) original.get("cnote");
				original.put("cnote", cnote + "," + fileName + "__" + value);
			} else {
				original.put("cnote", fileName + "__" + value);
			}
		}
	}

	/**
	 * 将一个map的所有value中的空格去除
	 * 
	 * @param map
	 * @return
	 */
	public static Map<String, Object> replaceSpace(Map<String, Object> map) {
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			if ("insertTime".equals(entry.getKey()) || "updateTime".equals(entry.getKey())
					|| "registeredTime".equals(entry.getKey()) || "loginTime".equals(entry.getKey())
					|| "newDate".equals(entry.getKey()) || "birthDay".equals(entry.getKey()))
				continue;
			entry.setValue((String.valueOf(entry.getValue())).replaceAll("\\s", "").replaceAll("", ""));
		}
		return map;
	}

}
