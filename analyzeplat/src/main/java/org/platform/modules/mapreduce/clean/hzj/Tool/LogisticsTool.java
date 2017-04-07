package org.platform.modules.mapreduce.clean.hzj.Tool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class LogisticsTool {
	//需要跳过的特殊字段的集合
	static List<String> limitList = new ArrayList<String>();
	static{
		limitList.add("updateTime");
		limitList.add("sourceFile");
		limitList.add("_id");
		limitList.add("insertTime");
		limitList.add("expressId");
		limitList.add("idCode");
		limitList.add("email");
		limitList.add("vipId");
		limitList.add("orderId");
		limitList.add("tbExpressId");
	}
	// 寻找符合指定字段的值
	public static List<String> findMatch(Map<String, Object> map, String param) {
		Iterator<Entry<String, Object>> newMap = map.entrySet().iterator();
		List<String> list = new ArrayList<String>();
		while (newMap.hasNext()) {
			Entry<String, Object> entry = newMap.next();
			// 获取当前遍历的key和value值
			String key = entry.getKey();
			String value = entry.getValue().toString();
			// 特殊字段跳过
			if (limitList.contains(key)) {
				continue;
			}
			if (("idCard".equals(param) && !"linkIdCard".equals(key))
					|| ("linkIdCard".equals(key) && !"idCard".equals(key))
					|| ("phone".equals(param) && !"linkPhone".equals(key)
							&& !"rcall".equals(key) && !"linkCall".equals(key)
							&& !"idCard".equals(key) && !"linkIdCard"
								.equals(key))
					|| ("rcall".equals(param) && !"linkPhone".equals(key)
							&& !"phone".equals(key) && !"linkCall".equals(key)
							&& !"idCard".equals(key) && !"linkIdCard"
								.equals(key))
					|| ("linkPhone".equals(param) && !"phone".equals(key)
							&& !"linkCall".equals(key) && !"rcall".equals(key)
							&& !"idCard".equals(key) && !"linkIdCard"
								.equals(key))
					|| ("linkCall".equals(param) && !"phone".equals(key)
							&& !"linkPhone".equals(key) && !"rcall".equals(key)
							&& !"idCard".equals(key) && !"linkIdCard"
								.equals(key))) {
				// 若在其他字段找到匹配的值，则取出匹配到的值
				List<String> list2 = findOther(param,value);
				if (list2!=null && list2.size()>0) {
					list.add(list2.get(0));
					list.add(key);
					list.add(list2.get(1));
					break;
				}
			}
		}
		return list;
	}

	/**
	 * 将找出匹配值得字段的其他值，放入address或linkAddress
	 * 
	 * @param map
	 *            原有数据的集合
	 * @param param
	 *            字段名
	 * @param value
	 *            需要添加到address或linkAddress的值
	 * @return
	 */
	public static Map<String, Object> addCnote(Map<String, Object> map,
			String param, String value) {
		if ("phone".equals(param) || "rcall".equals(param)
				|| "idCard".equals(param)) {
			
			if (map.containsKey("cnote")) {
				String cnoteValue = (String)map.get("cnote");
				if(!"".equals(value) && !"NA".equals(cnoteValue)){
					StringBuffer newValue = new StringBuffer();
					newValue.append(map.get("cnote") + "," + value);
					map.put("cnote", newValue);
				}
			} else {
				if (!"".equals(value)) {
					map.put("cnote", value);
				}
			}
		}
		if ("linkPhone".equals(param) || "linkCall".equals(param)
				|| "linkIdCard".equals(param)) {
			String cnoteValue = (String) map.get("cnote");
			if (map.containsKey("cnote") && !"".equals(value) && !"NA".equals(cnoteValue)){
				StringBuffer newValue = new StringBuffer();
				newValue.append(map.get("cnote") + value);
				map.put("cnote", newValue);
			} else {
				if (!"".equals(value)) {
					map.put("cnote", value);
				}
			}
		}
		if ("email".equals(param)) {
			String cnoteValue = (String) map.get("cnote");
			if (map.containsKey("cnote") && !"".equals(value) && !"NA".equals(cnoteValue)){
				StringBuffer newValue = new StringBuffer();
				newValue.append(map.get("cnote") + value);
				map.put("cnote", newValue);
			} else {
				if (!"".equals(value)) {
					map.put("cnote", value);
				}
			}
		}
		
		
		
		return map;
	}

	/**
	 * 判断字段中是否还有其他值
	 * 
	 * @param param
	 *            字段名
	 * @param value
	 *            字段的值
	 * @return
	 */
	public static List<String> findOther(String param, String value) {
		String phoneRex = CleanUtil.phoneRex;
		String idCardRex = CleanUtil.idCardRex;
		String callRex = CleanUtil.callRex;
		String emailRex = CleanUtil.emailRex;
		List<String> list = new ArrayList<String>();
		if ("idCard".equals(param) || "linkIdCard".equals(param)) {
			Pattern p = Pattern.compile(idCardRex);
			String correct = null;
			Matcher m = p.matcher(value);
			if (m.find()) {
				correct = value.replace(m.group(), "");
				list.add(m.group());
				list.add(correct);
				return list;
			}
		}
		
		
		
		// 判断座机中是否有 非座机内容（必须先判断是否含有手机号码，并且部分座机有括号）
		if ("rcall".equals(param) || "linkCall".equals(param) || "phone".equals(param) || "linkPhone".equals(param)) {
			Pattern p = Pattern.compile(phoneRex);
			Matcher m = p.matcher(value);
			Pattern pCall = Pattern.compile(callRex);
			StringBuffer str = new StringBuffer();
			String newValue = null;

			if (CleanUtil.matchPhone(value)) {
				while (m.find()) {
					if (str.length() == 0) {
						str.append(m.group());
						newValue = value.replace(m.group(), "");
					} else {
						str.append("," + m.group());
					}
					newValue = newValue.replace(m.group(), "");
				}
				if (newValue != null) {
					Matcher mCall = pCall.matcher(newValue);
					while (mCall.find()) {
						str.append("," + mCall.group());
						newValue = newValue.replace(mCall.group(), "");
					}
					if (str.length() != 0) {
						list.add(str.toString());
						list.add(newValue);
						return list;
					}
				} else {
					Matcher mCall = pCall.matcher(value);
					while (mCall.find()) {
						if (str.length() == 0) {
							str.append(mCall.group());
							newValue = value.replace(mCall.group(), "");
						} else {
							str.append("," + mCall.group());
							newValue = newValue.replace(mCall.group(), "");
						}
					}
					if (str.length() != 0) {
						list.add(str.toString());
						list.add(newValue);
						return list;
					}
				}
			}
			if (CleanUtil.matchCall(value)) {
				Matcher mCall = pCall.matcher(value);
				while (mCall.find()) {
					if (str.length() == 0) {
						str.append(mCall.group());
						newValue = value.replace(mCall.group(), "");
					} else {
						str.append("," + mCall.group());
					}
					newValue = newValue.replace(mCall.group(), "");
				}
				if (str.length() != 0) {
					list.add(str.toString());
					list.add(newValue);
					return list;
				}
			}
		}
		
		
		if ("email".equals(param)) {
			Pattern p = Pattern.compile(emailRex);
			String correct = null;
			Matcher m = p.matcher(value);
			if (m.find()) {
				correct = value.replace(m.group(), "");
				list.add(m.group());
				list.add(correct);
				return list;
			}
		}
		return null;
	}
}
