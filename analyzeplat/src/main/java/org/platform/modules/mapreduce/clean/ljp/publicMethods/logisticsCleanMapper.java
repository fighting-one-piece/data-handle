package org.platform.modules.mapreduce.clean.ljp.publicMethods;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;

public class logisticsCleanMapper {

	public static List<String> SkipTheFields() {
		List<String> continueList = new ArrayList<String>();
		continueList.add("_id");
		continueList.add("insertTime");
		continueList.add("sourceFile");
		continueList.add("updateTime");
		continueList.add("expressId");
		continueList.add("orderId");
		continueList.add("tbExpressId");
		continueList.add("expressContent");
		continueList.add("email");
		continueList.add("linkIdCard");
		continueList.add("linkEmail");
		continueList.add("bankCard");
		continueList.add("idCode");
		continueList.add("networkCode");
		continueList.add("rcall");
		continueList.add("linkCall");
		continueList.add("vipId");
		continueList.add("linkPhone");
		continueList.add("linkClientCode");
		continueList.add("phone");
		continueList.add("idCard");
		continueList.add("monthlyStatement");
		continueList.add("tbOrderId");
		continueList.add("businessPlatform");
		continueList.add("expressCompanyId");
		continueList.add("site");
		continueList.add("begainTime");
		continueList.add("cancelOrderTime");
		continueList.add("pieceTime");
		continueList.add("endDate");
		continueList.add("cnote");
		return continueList;
	}

	public static List<String> erroList() {
		List<String> erroList = new ArrayList<String>();
		erroList.add("NA");
		erroList.add("NULL");
		erroList.add("");
		erroList.add("null");
		return erroList;
	}

	public static List<String> replaceList() {
		List<String> replaceList = new ArrayList<String>();
		replaceList.add("begainTime");
		replaceList.add("endTime");
		replaceList.add("orderDate");
		replaceList.add("orderTime");
		replaceList.add("cancelOrderTime");
		replaceList.add("pieceTime");
		replaceList.add("endDate");
		return replaceList;
	}

	public static List<String> chineseAndEnglishList() {
		List<String> chineseAndEnglishList = new ArrayList<String>();
		chineseAndEnglishList.add("address");
		chineseAndEnglishList.add("linkAddress");
		chineseAndEnglishList.add("province");
		chineseAndEnglishList.add("city");
		chineseAndEnglishList.add("county");
		chineseAndEnglishList.add("linkProvince");
		chineseAndEnglishList.add("linkCity");
		chineseAndEnglishList.add("linkCounty");
		chineseAndEnglishList.add("receiptCompany");
		chineseAndEnglishList.add("linkSex");
		chineseAndEnglishList.add("sendCompany");
		chineseAndEnglishList.add("receiptCompany");
		return chineseAndEnglishList;
	}

	/**
	 * 清洗需要移除的跳过字段
	 * 
	 * @param indexName
	 * @return
	 */
	public static String[] removeList(String indexName) {
		List<String> removeList = SkipTheFields();
		for (int i = 0; i < removeList.size(); i++) {
			if (removeList.get(i).equals(indexName)) {
				removeList.remove(i);
			}
		}
		return removeList.toArray(new String[0]);
	}

	/**
	 * 跳过字段的动态数组
	 * 
	 * @return
	 */
	public static String[] continueArray() {
		return SkipTheFields().toArray(new String[0]);
	}

	/**
	 * 将废值替换为NA
	 * 
	 * @param field
	 *            字段值
	 * @param fieldName
	 *            字段名
	 * @param original
	 *            总值Map
	 */
	public static void judge(String field, String fieldName, Map<String, Object> original) {
		if (!"NA".equals(field) && StringUtils.isNotBlank(field)) {
			addCnote(original, field, fieldName);
		}
		original.put(fieldName, "NA");
	}

	/**
	 * 物流去除空格
	 * 
	 * @param map
	 * @return
	 */
	public static Map<String, Object> logisticsReplaceSpace(Map<String, Object> map) {
		for (Entry<String, Object> entry : map.entrySet()) {
			if ("insertTime".equals(entry.getKey()) || "updateTime".equals(entry.getKey())
					|| "begainTime".equals(entry.getKey()) || "endTime".equals(entry.getKey())
					|| "orderDate".equals(entry.getKey()) || "orderTime".equals(entry.getKey())
					|| "pieceTime".equals(entry.getKey()) || "endDate".equals(entry.getKey()))
				continue;
			entry.setValue((String.valueOf(entry.getValue())).replaceAll("\\s", "").replaceAll("", ""));
		}
		return map;
	}

	/**
	 * 根据传入的值，判断是否加入cnote
	 */
	public static void addCnote(Map<String, Object> original, String value, String fileName) {
		if (value != null && !logisticsCleanMapper.erroList().contains(value)) {
			if (original.containsKey("cnote")) {
				String cnote = (String) original.get("cnote");
				original.put("cnote", cnote + "," + fileName + "__" + value);
			} else {
				original.put("cnote", fileName + "__" + value);
			}
		}
	}

	/**
	 * 匹配含有中英文的字段
	 * 
	 * @param original
	 * @param str
	 */
	public static void boolChinese(Map<String, Object> original, String str) {
		String address = (String) original.get(str);
		if (original.containsKey(str) && (!CleanUtil.matchChinese(address) && !CleanUtil.matchEnglish(address))
				|| CleanUtil.matchNum(address)) {
			logisticsCleanMapper.judge(address, str, original);
		}
	}

	/**
	 * 更改时间格式
	 * 
	 * @param original
	 * @return
	 */
	public static Map<String, Object> dateFormat(Map<String, Object> original) {
		for (String timeName : replaceList()) {
			if (original.containsKey(timeName)) {
				String timeNameData = (String) original.get(timeName);
				original.put(timeName, !"".equals(timeNameData) && timeNameData != null
						? DateValidJob.FormatDate(timeNameData.trim()) : "NA");
			}
		}
		return original;
	}

	/**
	 * 删除废值的字段
	 * 
	 * @param map
	 * @return
	 */
	public static Map<String, Object> replaceMap(Map<String, Object> map) {
		Map<String, Object> temporaryMap = new HashMap<String, Object>();
		temporaryMap.putAll(map);
		for (Entry<String, Object> entry : temporaryMap.entrySet()) {
			if ("".equals(entry.getValue()) || "NA".equals(entry.getValue()) || "NULL".equals(entry.getValue())
					|| "null".equals(entry.getValue()) || entry.getValue() == null) {
				map.remove(entry.getKey());
			}
		}
		return map;
	}

	/**
	 * 判断price是否为数字
	 */
	public static Map<String, Object> priceJudge(Map<String, Object> original) {
		String price = (String) original.get("price");
		if (CleanUtil.matchNum(price)) {
			return original;
		} else {
			Object value = original.remove("price");
			if (!"NA".equals(value) && StringUtils.isNotBlank(value.toString())) {
				logisticsCleanMapper.addCnote(original, (String) original.get("price"), "price");
			}
			return original;
		}
	}

	/**
	 * 电话交换
	 * 
	 * @return
	 */
	public static void phoneExchange(Map<String, Object> original, String fileName,
			String exchangePhoneName) {
		if (original.containsKey(fileName)) {
			String fileData = (String) original.get(fileName);
			if (!CleanUtil.matchPhone(fileData) && !CleanUtil.matchCall(fileData)
					&& original.containsKey(exchangePhoneName)) {
				String exchangePhoneData = (String) original.get(exchangePhoneName);
				original.put(fileName, exchangePhoneData);
			}
		} else {
			if (original.containsKey(exchangePhoneName)) {
				String exchangePhoneData = (String) original.get(exchangePhoneName);
				original.put(fileName, exchangePhoneData);
			}
		}
	}

	/**
	 * 清洗后替换的字段
	 * 
	 * @param original
	 * @return
	 */
	public static Map<String, Object> keyReplace(Map<String, Object> original) {
		if (original.containsKey("linkPhone")) {
			original.put("linkMobilePhone", original.get("linkPhone"));
			original.remove("linkPhone");
		}
		if (original.containsKey("linkCall")) {
			original.put("linkTelePhone", original.get("linkCall"));
			original.remove("linkCall");
		}
		if (original.containsKey("phone")) {
			original.put("mobilePhone", original.get("phone"));
			original.remove("phone");
		}
		if (original.containsKey("rcall")) {
			original.put("telePhone", original.get("rcall"));
			original.remove("rcall");
		}
		if (original.containsKey("bankCard")) {
			original.put("cardNo", original.get("bankCard"));
			original.remove("bankCard");
		}
		if (original.containsKey("name")) {
			original.put("nameAlias", original.get("name"));
		}
		if (original.containsKey("linkName")) {
			original.put("linkNameAlias", original.get("linkName"));
		}
		return original;
	}

}
