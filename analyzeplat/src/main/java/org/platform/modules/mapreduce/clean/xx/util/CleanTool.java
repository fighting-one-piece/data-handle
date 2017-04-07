package org.platform.modules.mapreduce.clean.xx.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

/**
 * 清洗工具类
 * 
 * @author xiexin
 */
public class CleanTool {
	/**
	 * 处理存在且满足但是不规则字段
	 * 
	 * @param original
	 * @param fileName
	 * @param filedValue
	 * @param pattern
	 * @param reg
	 * @return Map
	 */
	public static Map<String, Object> clean(Map<String, Object> original, String fileName, String filedValue,
			Pattern pattern, String reg) {

		StringBuilder sBuilder = new StringBuilder();
		Matcher matcher = pattern.matcher(filedValue);
		while (matcher.find()) {
			sBuilder.append(matcher.group() + ",");
		}
		if (sBuilder.length() > 0) {
			sBuilder.deleteCharAt(sBuilder.length() - 1);
		}
		original.put(fileName, sBuilder.toString());
		if (StringUtils.isNotBlank(filedValue.replaceAll(reg, ""))
				&& !"NA".equals(StringUtils.isNotBlank(filedValue.replaceAll(reg, "")))) {
			if (original.containsKey("cnote")) {
				String note = (String) original.get("cnote");
				if (StringUtils.isNotBlank(note) && "NA".equals(note)) {
					original.put("cnote", note + "," + filedValue.replaceAll(reg, ""));
				} else {
					original.put("cnote", filedValue.replaceAll(reg, ""));
				}
			} else {
				original.put("cnote", filedValue.replaceAll(reg, ""));
			}
		}
		return original;
	}

	/**
	 * 处理存在且满足但是错列字段
	 * 
	 * @param original
	 * @param pattern
	 * @param reg
	 * @param fileName
	 * @param fileValue
	 * @return
	 */
	public static boolean cleanT(Map<String, Object> original, String fileName, Pattern pattern, String reg,
			String type) {
		boolean flag = false;
		StringBuilder sBuilder = new StringBuilder();
		for (Map.Entry<String, Object> entry : original.entrySet()) {
			if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
					|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
				continue;
			if ("car".equals(type)) {
				// 清洗车主类判断
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("frameNum") || entry.getKey().equals("engineNum")
							|| entry.getKey().equals("insuranceNum") || entry.getKey().equals("phone")
							|| entry.getKey().equals("callNo"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("frameNum")
							|| entry.getKey().equals("engineNum") || entry.getKey().equals("insuranceNum"))
						continue;
				}
				if ("callNo".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("engineNum") || entry.getKey().equals("insuranceNum")
							|| entry.getKey().equals("frameNum"))
						continue;
				}
				// 清洗物业
			} else if ("house".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("communityNum") || entry.getKey().equals("bankCard")
							|| entry.getKey().equals("contractNo") || entry.getKey().equals("saleNum")
							|| entry.getKey().equals("policeLocation"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("communityNum")
							|| entry.getKey().equals("bankCard") || entry.getKey().equals("contractNo")
							|| entry.getKey().equals("saleNum") || entry.getKey().equals("policeLocation"))
						continue;
				}
				if ("callNo".equals(fileName)) {
					if (entry.getKey().equals("communityNum") || entry.getKey().equals("bankCard")
							|| entry.getKey().equals("contractNo") || entry.getKey().equals("saleNum")
							|| entry.getKey().equals("policeLocation") || entry.getKey().equals("phone")
							|| entry.getKey().equals("fax") || entry.getKey().equals("postCode"))
						continue;
				}
				// 清洗邮箱
			} else if ("email".equals(type)) {
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("ip") || entry.getKey().equals("email"))
						continue;
				}
				if ("callNo".equals(fileName)) {
					if (entry.getKey().equals("phone") || entry.getKey().equals("provinceId")
							|| entry.getKey().equals("cityId") || entry.getKey().equals("ip")
							|| entry.getKey().equals("email"))
						continue;
				}
				// 清洗通讯录
			} else if ("Contact".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("companyPhone") || entry.getKey().equals("phone")
							|| entry.getKey().equals("homeCall"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("homeCall")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("email"))
						continue;
				}
				if ("homeCall".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("email"))
						continue;
				}
				// 清洗金融
			} else if ("finance".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("companyPhone") || entry.getKey().equals("phone")
							|| entry.getKey().equals("homeCall") || entry.getKey().equals("cardNumber"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("homeCall")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("email")
							|| entry.getKey().equals("cardNumber"))
						continue;
				}
				if ("homeCall".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("email")
							|| entry.getKey().equals("cardNumber") || entry.getKey().equals("fax")
							|| entry.getKey().equals("birthday"))
						continue;
				}
				if ("companyPhone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("homeCall") || entry.getKey().equals("email")
							|| entry.getKey().equals("cardNumber"))
						continue;
				}
				// 清洗老人数据
			} else if ("elder".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("mobilePhone") || entry.getKey().equals("homeCall"))
						continue;
				}
				if ("mobilePhone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("homeCall"))
						continue;
				}
				if ("homeCall".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("mobilePhone"))
						continue;
				}
			}
			String value = (String) entry.getValue();
			Matcher matcher = pattern.matcher(value);
			if (matcher.find()) { // 判断是否匹配
				flag = true;
				// 先交换
				String corr = value;
				String inco = (String) original.get(fileName);
				// 再清洗
				Matcher m = pattern.matcher(corr);
				while (m.find()) {
					sBuilder.append(m.group() + ",");
				}
				if (sBuilder.length() > 0) {
					sBuilder.deleteCharAt(sBuilder.length() - 1);
				}
				// 将提取后的信息存入cNote
				if (StringUtils.isNotBlank(corr.replaceAll(reg, "")) && "NA".equals(corr.replaceAll(reg, ""))) {
					if (original.containsKey("cnote")) {
						String note = (String) original.get("cnote");
						if (StringUtils.isNotBlank(note) && "NA".equals(note)) {
							original.put("cnote", note + "," + corr.replaceAll(reg, ""));
						} else {
							original.put("cnote", corr.replaceAll(reg, ""));
						}
					} else {
						original.put("cnote", corr.replaceAll(reg, ""));
					}
				}
				entry.setValue(inco);
				original.put(fileName, sBuilder.toString());
				break;
			}
		}
		return flag;
	}

	/**
	 * 字段不存在但是满足
	 * 
	 * @param original
	 * @param pattern
	 * @param reg
	 * @param fileName
	 * @return
	 */
	public static boolean cleanS(Map<String, Object> original, Pattern pattern, String reg, String fileName,
			String type) {
		boolean flag = false;
		StringBuilder sBuilder = new StringBuilder();
		for (Map.Entry<String, Object> entry : original.entrySet()) {
			if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
					|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
				continue;
			if ("car".equals(type)) {
				// 清洗车主类判断
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("frameNum") || entry.getKey().equals("engineNum")
							|| entry.getKey().equals("insuranceNum") || entry.getKey().equals("callNo"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("frameNum")
							|| entry.getKey().equals("engineNum") || entry.getKey().equals("insuranceNum"))
						continue;
				}
				if ("callNo".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("engineNum") || entry.getKey().equals("insuranceNum")
							|| entry.getKey().equals("frameNum"))
						continue;
				}
			} else if ("house".equals(type)) {

				// 清洗物业
				if ("idCard".equals(fileName)) {
					// System.out.println("card");
					if (entry.getKey().equals("communityNum") || entry.getKey().equals("bankCard")
							|| entry.getKey().equals("saleNum") || entry.getKey().equals("policeLocation"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("communityNum")
							|| entry.getKey().equals("bankCard") || entry.getKey().equals("contractNo")
							|| entry.getKey().equals("saleNum") || entry.getKey().equals("policeLocation"))
						continue;
				}
				if ("callNo".equals(fileName)) {
					if (entry.getKey().equals("communityNum") || entry.getKey().equals("bankCard")
							|| entry.getKey().equals("contractNo") || entry.getKey().equals("saleNum")
							|| entry.getKey().equals("policeLocation") || entry.getKey().equals("phone")
							|| entry.getKey().equals("fax") || entry.getKey().equals("idCard"))
						continue;
				}

			} else if ("email".equals(type)) {
				// 清洗通讯录
			} else if ("Contact".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("companyPhone") || entry.getKey().equals("phone")
							|| entry.getKey().equals("homeCall"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("homeCall")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("email"))
						continue;
				}
				if ("homeCall".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("email"))
						continue;
				}
				// 清洗金融
			} else if ("finance".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("companyPhone") || entry.getKey().equals("cardNumber")
							|| entry.getKey().equals("phone") || entry.getKey().equals("homeCall"))
						continue;
				}
				if ("phone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("companyPhone")
							|| entry.getKey().equals("cardNumber"))
						continue;
				}
				if ("homeCall".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("cardNumber")
							|| entry.getKey().equals("birthday"))
						continue;
				}
				if ("companyPhone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("cardNumber"))
						continue;
				}
			//清洗老人	
			} else if ("elder".equals(type)) {
				if ("idCard".equals(fileName)) {
					if (entry.getKey().equals("mobilePhone") || entry.getKey().equals("homeCall"))
						continue;
				}
				if ("mobilePhone".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("homeCall"))
						continue;
				}
				if ("homeCall".equals(fileName)) {
					if (entry.getKey().equals("idCard") || entry.getKey().equals("mobilePhone"))
						continue;
				}
			}
			String value = (String) entry.getValue();
			Matcher matcher = pattern.matcher(value);
			/*
			 * // 强制处理telphone会匹配日期yyyymmdd格式 String DataFromRex3 =
			 * "[0-9]{4}[0-1]{1}[0-9]{1}([0-3]{1}|[0-3]{0})[0-9]{1}"; Pattern
			 * patternCall = Pattern.compile(DataFromRex3); Matcher matche1r =
			 * patternCall.matcher(value);
			 */
			if (matcher.find()) { // 判断是否匹配
				flag = true;
				Matcher m = pattern.matcher(value);
				while (m.find()) {
					sBuilder.append(m.group() + ",");
				}
				if (sBuilder.length() > 0) {
					sBuilder.deleteCharAt(sBuilder.length() - 1);
				}
				// if (matche1r.find() && "homeCall".equals(fileName)) {
				if (sBuilder.indexOf(",") > 0) {
					sBuilder.delete(sBuilder.indexOf(","), sBuilder.length());
				} else {
					sBuilder.delete(0, sBuilder.length());
				}
				// }
				// 将提取后的信息存入cNote
				if (StringUtils.isNotBlank(value.replaceAll(reg, "")) && !"NA".equals(value.replaceAll(reg, ""))) {
					if (original.containsKey("cnote")) {
						String note = (String) original.get("cnote");
						if (StringUtils.isNotBlank(note) && "NA".equals(note)) {
							original.put("cnote", note + "," + value.replaceAll(reg, ""));
						} else {
							original.put("cnote", value.replaceAll(reg, ""));
						}
					} else {
						original.put("cnote", value.replaceAll(reg, ""));
					}
				}
				// System.out.println(fileName+sBuilder.toString());
				original.put(fileName, sBuilder.toString());
				entry.setValue(""); // 最后处理空字段
				// System.out.println("end: "+original.toString());
				break;
			}

		}
		return flag;
	}

	/**
	 * 字段存在但是都不满足
	 * 
	 * @param original
	 * @param fileName
	 * @return
	 */
	public static Map<String, Object> cleanK(Map<String, Object> original, String fileName) {
		String file = (String) original.get(fileName);
		if (StringUtils.isNotBlank(file) && !"NA".equals(file)) {
			if (original.containsKey("cnote")) {
				String note = (String) original.get("cnote");
				if (StringUtils.isNotBlank(note) && !"NA".equals(note)) {
					original.put("cnote", note + "," + file);
				} else {
					original.put("cnote", file);
				}
			} else {
				original.put("cnote", file);
			}
		}
		original.remove(fileName);
		return null;
	}

	/**
	 * 新旧字段替换
	 * 
	 * @param orinal
	 * @return
	 */
	public static Map<String, Object> Transitio(Map<String, Object> orinal) {

		if (orinal.containsKey("name")) {
			orinal.put("nameAlias", orinal.get("name"));
		}
		if (orinal.containsKey("nameSecond")) {
			orinal.put("nameSecondAlias", orinal.get("nameSecond"));
		}
		if (orinal.containsKey("phone")) {
			orinal.put("mobilePhone", orinal.get("phone"));
			orinal.remove("phone");
		}
		if (orinal.containsKey("companyPhone")) {
			orinal.put("companyMobilePhone", orinal.get("companyPhone"));
			orinal.remove("companyPhone");
		}
		if (orinal.containsKey("homeCall")) {
			orinal.put("telePhone", orinal.get("homeCall"));
			orinal.remove("homeCall");
		}
		if (orinal.containsKey("callNo")) {
			orinal.put("telePhone", orinal.get("callNo"));
			orinal.remove("callNo");
		}
		if (orinal.containsKey("contantNo")) {
			orinal.put("contactPhone", orinal.get("contantNo"));
			orinal.remove("contantNo");
		}
		if (orinal.containsKey("companyName")) {
			orinal.put("company", orinal.get("companyName"));
			orinal.remove("companyName");
		}
		if (orinal.containsKey("qq")) {
			orinal.put("qqNum", orinal.get("qq"));
			orinal.remove("qq");
		}
		if (orinal.containsKey("nation")) {
			orinal.put("nationality", orinal.get("qq"));
			orinal.remove("nation");
		}
		if (orinal.containsKey("cardNumber")) {
			orinal.put("cardNo", orinal.get("cardNumber"));
			orinal.remove("cardNumber");
		}
		if (orinal.containsKey("nativePlace")) {
			orinal.put("birthPlace", orinal.get("nativePlace"));
			orinal.remove("nativePlace");
		}
		if (orinal.containsKey("birthday")) {
			orinal.put("birthDay", orinal.get("birthday"));
			orinal.remove("birthday");
		}
		if (orinal.containsKey("remark")) {
			orinal.put("note", orinal.get("remark"));
			orinal.remove("remark");
		}
		if (orinal.containsKey("webSite")) {
			orinal.put("website", orinal.get("webSite"));
			orinal.remove("webSite");
		}
		if (orinal.containsKey("passWord")) {
			orinal.put("password", orinal.get("passWord"));
			orinal.remove("passWord");
		}
		if (orinal.containsKey("postCode")) {
			orinal.put("zipCode", orinal.get("postCode"));
			orinal.remove("postCode");
		}
		if (orinal.containsKey("bankCard")) {
			orinal.put("cardNo", orinal.get("bankCard"));
			orinal.remove("bankCard");
		}
		if (orinal.containsKey("user")) {
			orinal.put("userName", orinal.get("user"));
			orinal.remove("user");
		}
		return orinal;
	}

	/**
	 * 处理空字段 如果为空(NA、null...)则直接删除
	 * 
	 * @param original
	 * @return
	 */
	public static Map<String, Object> cleanNull(Map<String, Object> original) {
		Iterator<Entry<String, Object>> it = original.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			String value = (String) entry.getValue();
			if (StringUtils.isBlank(value) || "NA".equals(value) || "NULL".equals(value)) {
				it.remove();
			}
		}
		return original;
	}

	/**
	 * 处理空格
	 * 
	 * @param map
	 * @return
	 */
	public static Map<String, Object> replaceSpace(Map<String, Object> map) {
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			if ("insertTime".equals(entry.getKey()) || "updateTime".equals(entry.getKey()))
				continue;
			entry.setValue((String.valueOf(entry.getValue())).replaceAll("	", ",").replaceAll(" ", ","));
		}
		return map;
	}

	/**
	 * 判断数据是否符合要求
	 * 
	 * @param original
	 * @return
	 */
	public static Boolean cleanX(Map<String, Object> original) {
		boolean flag = false;
		if (original.containsKey("idCard")) {
			flag = true;
		} else if (original.containsKey("phone")) {
			flag = true;
		} else if (original.containsKey("mobilePhone")) {
			flag = true;
		} else if (original.containsKey("callNo")) {
			flag = true;
		} else if (original.containsKey("licensePlate")) {
			flag = true;
		} else if (original.containsKey("frameNum")) {
			flag = true;
		} else if (original.containsKey("engineNum")) {
			flag = true;
		} else if (original.containsKey("insuranceNum")) {
			flag = true;
		} else if (original.containsKey("name") && original.containsKey("address")) {
			String name = (String) original.get("name");
			if (CleanUtil.matchNum(name)) {
				cleanK(original, "name");
			}
			flag = true;
		} else if (original.containsKey("homeCall")) {
			flag = true;
		} else if (original.containsKey("companyPhone")) {
			flag = true;
		} else if (original.containsKey("email")) {
			flag = true;
		} else if (original.containsKey("qq")) {
			flag = true;
		} else if (original.containsKey("msn")) {
			flag = true;
		} else if (original.containsKey("cardNumber") && original.containsKey("name")) {
			flag = true;
		}
		if (original.size() <= 5) {
			flag = false;
		}
		return flag;
	}
}
