package org.platform.modules.mapreduce.clean.ljp.HDFS2HDFS.V1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class HealthproductsHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return HealthproductsHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {

		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(records)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				sb.deleteCharAt(sb.length() - 1);
				System.out.println("i>>" + i);
				System.out.println("路径:>>" + sb.toString());
				String[] str = new String[]{"",sb.toString(),args[1]+i};
				exitCode = ToolRunner.run(new HealthproductsHDFS2HDFSJob(), str);
				sb = new StringBuilder();
			}
			long timeEnd = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class HealthproductsHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {
	static List<String> list = new ArrayList<String>();
	static {
		list.add("_id");
		list.add("insertTime");
		list.add("sourceFile");
		list.add("updateTime");
		list.add("expressId");
		list.add("orderId");
		list.add("expressContent");
		list.add("tbExpressId");
		list.add("linkClienteleCoding");
		list.add("idCode");
		list.add("linkMobilePhone");
		list.add("linkTelePhone");
		list.add("mobilePhone1");
		list.add("mobilePhone2");
		list.add("mobilePhone3");
		list.add("mobilePhone4");
		list.add("mobilePhone5");
		list.add("telePhone1");
		list.add("telePhone2");
		list.add("telePhone3");
		list.add("cnote");
	}

	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		int size = 0;
		boolean flag = false;

		// 将所有的字段去空格
		CleanUtil.replaceSpace(original);

		/**
		 * mobilePhone1
		 */

		if (original.containsKey("mobilePhone1")) {
			String mobilePhone1 = (String) original.get("mobilePhone1");
			if (CleanUtil.isAllHalf(mobilePhone1)) {
				mobilePhone1 = CleanUtil.ToDBC(mobilePhone1);
			}
			if (CleanUtil.matchPhone(mobilePhone1) || CleanUtil.matchCall(mobilePhone1.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, mobilePhone1, "mobilePhone1");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("mobilePhone1");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "mobilePhone1");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("mobilePhone1"), "mobilePhone1", original);
				}

			}
		}
		/**
		 * mobilePhone2
		 */

		if (original.containsKey("mobilePhone2")) {
			String mobilePhone2 = (String) original.get("mobilePhone2");
			if (CleanUtil.isAllHalf(mobilePhone2)) {
				mobilePhone2 = CleanUtil.ToDBC(mobilePhone2);
			}
			if (CleanUtil.matchPhone(mobilePhone2) || CleanUtil.matchCall(mobilePhone2.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, mobilePhone2, "mobilePhone2");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("mobilePhone2");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "mobilePhone2");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("mobilePhone2"), "mobilePhone2", original);
				}

			}
		}
		/**
		 * mobilePhone3
		 */

		if (original.containsKey("mobilePhone3")) {
			String mobilePhone3 = (String) original.get("mobilePhone3");
			if (CleanUtil.isAllHalf(mobilePhone3)) {
				mobilePhone3 = CleanUtil.ToDBC(mobilePhone3);
			}
			if (CleanUtil.matchPhone(mobilePhone3) || CleanUtil.matchCall(mobilePhone3.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, mobilePhone3, "mobilePhone3");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("mobilePhone3");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "mobilePhone3");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("mobilePhone3"), "mobilePhone3", original);
				}

			}
		}
		/**
		 * mobilePhone4
		 */

		if (original.containsKey("mobilePhone4")) {
			String mobilePhone4 = (String) original.get("mobilePhone4");
			if (CleanUtil.isAllHalf(mobilePhone4)) {
				mobilePhone4 = CleanUtil.ToDBC(mobilePhone4);
			}
			if (CleanUtil.matchPhone(mobilePhone4) || CleanUtil.matchCall(mobilePhone4.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, mobilePhone4, "mobilePhone4");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("mobilePhone4");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "mobilePhone4");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("mobilePhone4"), "mobilePhone4", original);
				}

			}
		}
		/**
		 * mobilePhone5
		 */

		if (original.containsKey("mobilePhone5")) {
			String mobilePhone5 = (String) original.get("mobilePhone5");
			if (CleanUtil.isAllHalf(mobilePhone5)) {
				mobilePhone5 = CleanUtil.ToDBC(mobilePhone5);
			}
			if (CleanUtil.matchPhone(mobilePhone5) || CleanUtil.matchCall(mobilePhone5.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, mobilePhone5, "mobilePhone5");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("mobilePhone5");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "mobilePhone5");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("mobilePhone5"), "mobilePhone5", original);
				}

			}
		}

		/**
		 * linkMobilePhone
		 */
		if (original.containsKey("linkMobilePhone")) {
			String linkMobilePhone = (String) original.get("linkMobilePhone");
			if (CleanUtil.isAllHalf(linkMobilePhone)) {
				linkMobilePhone = CleanUtil.ToDBC(linkMobilePhone);
			}
			if (CleanUtil.matchPhone(linkMobilePhone)
					|| CleanUtil.matchCall(linkMobilePhone.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, linkMobilePhone, "linkMobilePhone");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctlinkPhone = (String) entry.getValue();
						Object incorrectlinkPhone = original.get("linkMobilePhone");
						entry.setValue(incorrectlinkPhone);
						cleanReturnField(original, correctlinkPhone, "linkMobilePhone");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("linkMobilePhone"), "linkMobilePhone", original);
				}

			}

		} else {
			for (Entry<String, Object> entry : original.entrySet()) {
				if (list.contains(entry.getKey()))
					continue;
				if (CleanUtil.matchPhone((String) (entry.getValue()))
						|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
					String correctlinkPhone = (String) entry.getValue();
					cleanReturnField(original, correctlinkPhone, "linkMobilePhone");
					original.put(entry.getKey(), "NA");
					flag = true;
					size++;
					break;
				}
			}
		}

		/**
		 * telePhone1
		 */
		if (original.containsKey("telePhone1")) {
			String telePhone1 = (String) original.get("telePhone1");
			if (CleanUtil.isAllHalf(telePhone1)) {
				telePhone1 = CleanUtil.ToDBC(telePhone1);

			}
			if (CleanUtil.matchPhone(telePhone1) || CleanUtil.matchCall(telePhone1.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, telePhone1, "telePhone1");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctRcall = (String) entry.getValue();
						Object incorrectRcall = original.get("telePhone1");
						entry.setValue(incorrectRcall);
						cleanReturnField(original, correctRcall, "telePhone1");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("telePhone1"), "telePhone1", original);
				}

			}
		}
		/**
		 * telePhone2
		 */
		if (original.containsKey("telePhone2")) {
			String telePhone2 = (String) original.get("telePhone2");
			if (CleanUtil.isAllHalf(telePhone2)) {
				telePhone2 = CleanUtil.ToDBC(telePhone2);

			}
			if (CleanUtil.matchPhone(telePhone2) || CleanUtil.matchCall(telePhone2.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, telePhone2, "telePhone2");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctRcall = (String) entry.getValue();
						Object incorrectRcall = original.get("telePhone2");
						entry.setValue(incorrectRcall);
						cleanReturnField(original, correctRcall, "telePhone2");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("telePhone2"), "telePhone2", original);
				}

			}
		}
		/**
		 * telePhone3
		 */
		if (original.containsKey("telePhone3")) {
			String telePhone3 = (String) original.get("telePhone3");
			if (CleanUtil.isAllHalf(telePhone3)) {
				telePhone3 = CleanUtil.ToDBC(telePhone3);

			}
			if (CleanUtil.matchPhone(telePhone3) || CleanUtil.matchCall(telePhone3.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, telePhone3, "telePhone3");
				flag = true;
				size++;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctRcall = (String) entry.getValue();
						Object incorrectRcall = original.get("telePhone3");
						entry.setValue(incorrectRcall);
						cleanReturnField(original, correctRcall, "telePhone3");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("telePhone3"), "telePhone3", original);
				}

			}
		}

		/**
		 * linkTelePhone
		 */
		if (original.containsKey("linkTelePhone")) {
			String linkTelePhone = (String) original.get("linkTelePhone");
			if (CleanUtil.isAllHalf(linkTelePhone)) {
				linkTelePhone = CleanUtil.ToDBC(linkTelePhone);
			}
			if (CleanUtil.matchPhone(linkTelePhone) || CleanUtil.matchCall(linkTelePhone.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, linkTelePhone, "linkTelePhone");
				flag = true;
				size++;
			} else {

				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctlinkCall = (String) entry.getValue();
						Object incorrectlinkCall = original.get("linkTelePhone");
						entry.setValue(incorrectlinkCall);
						cleanReturnField(original, correctlinkCall, "linkTelePhone");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					judge((String) original.get("linkTelePhone"), "linkTelePhone", original);
				}

			}
		}

		/**
		 * 判断price是否是数字
		 */
		if (original.containsKey("price")) {
			String price = (String) original.get("price");
			if (CleanUtil.matchDouble(price)) {
				flag = true;
				size++;
			} else {
				Object value = original.remove("price");
				if (!"NA".equals(value) && StringUtils.isNotBlank(value.toString())) {
					if (original.containsKey("cnote")) {
						String cnote = ((String) original.get("cnote")).equals("NA") ? ""
								: (String) original.get("cnote");
						original.put("cnote", cnote + value);
					} else {
						original.put("cnote", value);
					}
				}
			}
		}

		/**
		 * 判断合格的姓名与地址,与合格的证件类型与证件号
		 */
		if (!flag) {
			if (((CleanUtil.matchChinese((String) original.get("name"))
					|| CleanUtil.matchEnglish((String) original.get("name")))
					&& (CleanUtil.matchChinese((String) original.get("address"))
							|| CleanUtil.matchChinese((String) original.get("city"))
							|| CleanUtil.matchChinese((String) original.get("province"))
							|| CleanUtil.matchChinese((String) original.get("county"))))
					|| ((CleanUtil.matchChinese((String) original.get("linkName"))
							|| CleanUtil.matchEnglish((String) original.get("linkName")))
							&& (CleanUtil.matchChinese((String) original.get("linkAddress"))
									|| CleanUtil.matchChinese((String) original.get("linkCity"))
									|| CleanUtil.matchChinese((String) original.get("linkProvince"))
									|| CleanUtil.matchChinese((String) original.get("linkCounty"))))) {
				flag = true;
			}
		}

		/**
		 * 判断特殊字段的存在
		 */
		for (Entry<String, Object> entry : original.entrySet()) {
			if ("id".equals(entry.getKey()) || "updateTime".equals(entry.getKey())
					|| "sourceFile".equals(entry.getKey()) || "insertTime".equals(entry.getKey())
					|| "inputPerson".equals(entry.getKey())) {
				size++;
			}
		}
		// 判断除NA之外的有效字段
		int corriSize = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			if (!"NA".equals((String) entry.getValue())) {
				corriSize++;
			}
		}

		// 字段数
		if (corriSize < size + 2) {
			flag = false;
		}

		// 放入结果集,并替换主要字段的key
		if (flag) {
			correct.putAll(keyReplace(original));
		} else {
			incorrect.putAll(map);
		}
	}

	/**
	 * 清洗idCard,linkidCard email,linkEmail
	 * 
	 * @param original
	 *            总数Map
	 * @param field
	 *            字段
	 * @param Match
	 *            匹配的正则表达式
	 * @param fieldName字段名
	 */
	public void cleanfield(Map<String, Object> original, String field, String Match, String fieldName) {
		Pattern pat = Pattern.compile(Match);
		Matcher M = pat.matcher(field);
		List<Object> listField = new ArrayList<Object>();
		while (M.find()) {
			listField.add(M.group());
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i));
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		String toBuffer = buffer.toString();
		String supersession = field.replaceAll(Match, "");
		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(fieldName, toBuffer);
	}

	/**
	 * 清洗Phone 与 Call
	 * 
	 * @param original
	 *            总数Map
	 * @param field
	 *            字段值
	 * @param Match
	 *            匹配的正则表达式
	 * @param fieldName
	 *            字段名
	 */
	public void cleanReturnField(Map<String, Object> original, String field, String fieldName) {
		List<Object> listField = new ArrayList<Object>();
		StringBuffer buffer = new StringBuffer();

		Pattern pat = Pattern.compile(CleanUtil.phoneRex);
		Matcher M = pat.matcher(field);
		while (M.find()) {
			listField.add(M.group());
		}

		String supersession = field.replaceAll(CleanUtil.phoneRex, "");

		pat = Pattern.compile(CleanUtil.callRex);
		M = pat.matcher(supersession);
		while (M.find()) {
			listField.add(M.group());
		}
		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i));
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		String toBuffer = buffer.toString();
		supersession = supersession.replaceAll(CleanUtil.callRex, "");

		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(fieldName, toBuffer);
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
	public void judge(String field, String fieldName, Map<String, Object> original) {
		if (!"NA".equals(field) && StringUtils.isNotBlank(field)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + field);
			} else {
				original.put("cnote", field);
			}
		}
		original.put(fieldName, "NA");
	}

	/**
	 * 清洗后替换的字段
	 * 
	 * @param original
	 * @return
	 */
	public Map<String, Object> keyReplace(Map<String, Object> original) {
		if (original.containsKey("name")) {
			original.put("nameAlias", original.get("name"));
		}
		if (original.containsKey("linkName")) {
			original.put("linkNameAlias", original.get("linkName"));
		}
		return original;
	}
}
