package org.platform.modules.mapreduce.clean.tjl;

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

/**
 * 账号数据清洗
 * 
 * @author tjl
 *
 */
public class AccountHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return AccountHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			long starTime = System.currentTimeMillis();
			// args = new
			// String[]{"hdfs://192.168.0.115:9000/elasticsearch/account/account",
			// "hdfs://192.168.0.115:9000/elasticsearch/work/tjl/new_account"};
			int exitCode = ToolRunner.run(new AccountHDFS2HDFSJob(), args);
			long endTime = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(endTime - starTime);
			System.out.println("用时:" + formatter.format(date));
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class AccountHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {

		int size = 5;

		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);

		original = CleanUtil.replaceSpace(original);

		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			if (CleanUtil.matchIdCard(idCard)) {
				cleanField(original, idCard, CleanUtil.idCardRex, "idCard");
			} else {
				original.put("idCard", "NA");
				if (original.containsKey("cnote")) {
					String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
					original.put("cnote", cnote + idCard);
				} else {
					original.put("cnote", idCard);
				}
			}
		}

		// 对email字段进行清洗，不正确直接设为NA
		if (original.containsKey("email")) {
			String email = (String) original.get("email");
			if (CleanUtil.matchEmail(email)) {
				email = cleanEmail(email);
				original.put("email", email);
			} else {
				original.put("email", "NA");
				if (original.containsKey("cnote")) {
					String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
					original.put("cnote", cnote + email);
				} else {
					original.put("cnote", email);
				}
			}
		}

		// 对phone字段进行清洗，不正确直接设为NA
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone.replaceAll("[(]|[)]", "-"))) {
				phone = cleanPhone(phone.replaceAll("[(]|[)]", "-"));
				original.put("phone", phone);
			} else {
				original.put("phone", "NA");
				if (original.containsKey("cnote")) {
					String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
					original.put("cnote", cnote + phone);
				} else {
					original.put("cnote", phone);
				}
			}
		}

		int sum = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			if (!"NA".equals((String) entry.getValue()) && !"".equals((String) entry.getValue())) {
				sum++;
			}
		}
		boolean flag = true;
		if (sum <= size) {
			flag = false;
		}
		if (flag) {
			correct.putAll(changeKey(original));
		} else {
			incorrect.putAll(map);
		}

	}

	/**
	 * 
	 * @param original
	 *            存储数据的Map
	 * @param field
	 *            字段值
	 * @param rex
	 *            匹配的正则表达式
	 * @param fieldName
	 *            字段名
	 */
	public void cleanField(Map<String, Object> original, String field, String rex, String fieldName) {
		Pattern pat = Pattern.compile(rex);
		Matcher M = pat.matcher(field);
		List<Object> listField = new ArrayList<Object>();
		while (M.find()) {
			listField.add(M.group()); // 将正确的数据放入集合里面
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		String toBuffer = buffer.toString();
		String supersession = field.replaceAll(rex, ""); // 将不匹配的元素取出来
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

	public String cleanIdCard(String field) {
		Pattern pat = Pattern.compile(CleanUtil.idCardRex);
		Matcher M = pat.matcher(field);
		List<Object> listField = new ArrayList<Object>();
		while (M.find()) {
			listField.add(M.group()); // 将正确的数据放入集合里面
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		return buffer.toString();
	}

	public String cleanEmail(String field) {
		Pattern pat = Pattern.compile(CleanUtil.emailRex);
		Matcher M = pat.matcher(field);
		List<Object> listField = new ArrayList<Object>();
		while (M.find()) {
			listField.add(M.group()); // 将正确的数据放入集合里面
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		return buffer.toString();
	}

	public String cleanPhone(String field) {
		Pattern pat = Pattern.compile(CleanUtil.phoneRex);
		Matcher M = pat.matcher(field);
		List<Object> listfield = new ArrayList<Object>();
		while (M.find()) {
			listfield.add(M.group()); // 将正确的数据放入集合里面
		}
		String supersession = field.replaceAll(CleanUtil.phoneRex, ""); // 将不匹配的元素替换掉
		pat = Pattern.compile(CleanUtil.callRex);
		M = pat.matcher(supersession);
		while (M.find()) {
			listfield.add(M.group()); // 将正确的数据放入集合里面
		}
		supersession = supersession.replaceAll(CleanUtil.callRex, "");
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listfield.size(); i++) {
			if (i == 0) {
				buffer.append(listfield.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listfield.get(i));
			}
		}

		return buffer.toString();
	}

	public Map<String, Object> changeKey(Map<String, Object> map) {
		if (map.containsKey("phone")) {
			Object value = map.get("phone");
			map.remove("phone");
			map.put("mobilePhone", value);
		}

		return map;
	}

}