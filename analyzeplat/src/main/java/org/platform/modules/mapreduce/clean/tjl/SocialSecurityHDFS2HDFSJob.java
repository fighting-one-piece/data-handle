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
 * 社保数据清洗
 * 
 * @author tjl
 *
 */
public class SocialSecurityHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {

		return SocialSecurityHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			long starTime = System.currentTimeMillis();
			String n;
			int exitCode = 0;
			for (int j = 1; j <= 1; j++) {
				for (int i = 0; i <= 9; i++) {
					if (i < 10) {
						n = "0" + i;
					} else {
						n = "" + i;
					}
					// args = new
					// String[]{"hdfs://192.168.0.115:9000/elasticsearch/tjl/ot/records-"+index+"-m-000"+n,
					// "hdfs://192.168.0.115:9000/elasticsearch/tjl/ot_out/records-"+index+"-m-000"+n};
					args = new String[] {"",
							"hdfs://192.168.0.10:9000/elasticsearch_original/work/socialsecurity/105/records-" + j + "-m-000" + n,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/socialsecurity/105/records-" + j + "-m-000"
									+ n };
					System.out.println(args[0] + ":" + args[1]);
					exitCode = ToolRunner.run(new SocialSecurityHDFS2HDFSJob(), args);
				}
			}
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

class SocialSecurityHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		int size = 5;
		boolean flag = false;
		// 清洗idCard字段
		// 判断是否包含idCard字段
		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			// 判断字段中是否包含正确的身份证号
			if (CleanUtil.matchIdCard(idCard)) {
				// 包含则将包含的身份证号提取出来放入idCard，提取后剩下的数据append到address上
				cleanField(original, idCard, CleanUtil.idCardRex, "idCard");
				flag = true;
			} else {// 如果不包含身份证号则可能错列
				boolean cleanFlag = false;
				// 遍历map，跳过特殊字段，判断是否有包含idCard的字段
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
						// || entry.getKey().equals("medicalCard") ||
						// entry.getKey().equals("personalNo")
						continue;
					// 如果有包含idCard的字段，则将其与idCard字段值进行交换
					if (CleanUtil.matchIdCard((String) (entry.getValue()))) {
						String correctIdCard = (String) entry.getValue();
						Object incorrectIdCard = original.get("idCard");
						entry.setValue(incorrectIdCard);
						// 身份证号提取出来放入idCard，提取后剩下的数据append到address上
						cleanField(original, correctIdCard, CleanUtil.idCardRex, "idCard");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				// 如果没有找到身份证号，则idcard字段为废值，将其值append到address上，并将idCard字段设为"NA"
				if (!cleanFlag) {
					size++;
					judge(idCard, "idCard", original);
				}
			}
		}

		// 由于存在两个手机号码用空格分割的情况，所以最后再替换空格
		original = CleanUtil.replaceSpace(original);

		// phone字段清洗 phone字段中可能是电话号码也可能是座机号码
		// 判断是否包含phone字段
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			// 如果字段中包含电话号码，则将电话号码提出来放入phone，剩下的值append到address
			if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone.replaceAll("[(]|[)]", "-"))) {
				cleanPhones(original, phone.replaceAll("[(]|[)]", "-"), "phone");
				flag = true;
				// 判断是否包含座机号码，由于座机号码正则表达式区号与号码分隔符为"-"，所以这里要将其它分隔符替换为"-"
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("medicalCard") || entry.getKey().equals("personalNo")
							|| entry.getKey().equals("socialSecurityNo") || entry.getKey().equals("socialSecurityCode")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("rcall")
							|| entry.getKey().equals("transactorTel") || entry.getKey().equals("companyTel"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = ((String) (entry.getValue())).replaceAll("[(]|[)]", "-");
						Object incorrectPhone = original.get("phone");
						entry.setValue(incorrectPhone);
						cleanPhones(original, correctPhone, "phone");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(phone, "phone", original);
				}
			}
		}

		if (original.containsKey("rcall")) {
			String rcall = (String) original.get("rcall");
			if (CleanUtil.matchPhone(rcall) || CleanUtil.matchCall(rcall.replaceAll("[(]|[)]", "-"))) {
				cleanPhones(original, rcall.replaceAll("[(]|[)]", "-"), "rcall");
				flag = true;
				// 判断是否包含座机号码，由于座机号码正则表达式区号与号码分隔符为"-"，所以这里要将其它分隔符替换为"-"
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("medicalCard") || entry.getKey().equals("personalNo")
							|| entry.getKey().equals("socialSecurityNo") || entry.getKey().equals("socialSecurityCode")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("transactorTel") || entry.getKey().equals("companyTel"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctRcall = ((String) (entry.getValue())).replaceAll("[(]|[)]", "-");
						Object incorrectRcall = original.get("rcall");
						entry.setValue(incorrectRcall);
						cleanPhones(original, correctRcall, "rcall");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(rcall, "rcall", original);
				}
			}
		}

		if (original.containsKey("companyTel")) {
			String companyTel = (String) original.get("companyTel");
			if (CleanUtil.matchPhone(companyTel) || CleanUtil.matchCall(companyTel.replaceAll("[(]|[)]", "-"))) {
				cleanPhones(original, companyTel.replaceAll("[(]|[)]", "-"), "phone");
				flag = true;
				// 判断是否包含座机号码，由于座机号码正则表达式区号与号码分隔符为"-"，所以这里要将其它分隔符替换为"-"
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("medicalCard") || entry.getKey().equals("personalNo")
							|| entry.getKey().equals("socialSecurityNo") || entry.getKey().equals("socialSecurityCode")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("rcall")
							|| entry.getKey().equals("transactorTel") || entry.getKey().equals("phone"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctCompanyTel = ((String) (entry.getValue())).replaceAll("[(]|[)]", "-");
						Object incorrectCompanyTel = original.get("companyTel");
						entry.setValue(incorrectCompanyTel);
						cleanPhones(original, correctCompanyTel, "companyTel");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(companyTel, "companyTel", original);
				}
			}
		}
		if (original.containsKey("transactorTel")) {
			String transactorTel = (String) original.get("transactorTel");
			if (CleanUtil.matchPhone(transactorTel) || CleanUtil.matchCall(transactorTel.replaceAll("[(]|[)]", "-"))) {
				cleanPhones(original, transactorTel.replaceAll("[(]|[)]", "-"), "transactorTel");
				flag = true;
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("medicalCard") || entry.getKey().equals("personalNo")
							|| entry.getKey().equals("socialSecurityNo") || entry.getKey().equals("socialSecurityCode")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("rcall")
							|| entry.getKey().equals("phone") || entry.getKey().equals("companyTel"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctTransactorTel = ((String) (entry.getValue())).replaceAll("[(]|[)]", "-");
						Object incorrectTransactorTel = original.get("transactorTel");
						entry.setValue(incorrectTransactorTel);
						cleanPhones(original, correctTransactorTel, "transactorTel");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(transactorTel, "transactorTel", original);
				}
			}
		}

		if (!flag) {
			if (original.containsKey("socialSecurityNo") && CleanUtil.matchNumAndLetter((String) original.get("socialSecurityNo"))) {
				flag = true;
			} else if (original.containsKey("name") && original.containsKey("address")
					&& CleanUtil.matchChinese((String) original.get("name"))
					&& CleanUtil.matchChinese((String) original.get("address")) ) {
				flag = true;
			} else if (original.containsKey("company") && original.containsKey("address")
					&& CleanUtil.matchChinese((String) original.get("company"))
					&& CleanUtil.matchChinese((String) original.get("address"))){
				flag = true;
			}
		}
		if (original.containsKey("zipCode")){
			String zipCode = (String) original.get("zipCode");
			if (!CleanUtil.matchNum(zipCode)){
				judge(zipCode, "zipCode", original);
			}
		}
		
		
		int corriSize = 0;
		for (Entry<String, Object> entry : original.entrySet()){
			if (!"NA".equals((String)entry.getValue())){
				corriSize++;
			}
		}
		
		if (original.size() <= size || corriSize <= 5){
			flag = false;
		}

		// 有正确的主要字段，且包含其它字段时为正确数据
		if (flag) {
			correct.putAll(changeKey(original));
		} else {
			incorrect.putAll(map);
		}

	}

	/**
	 * 将废值替换为NA
	 * 
	 * @param field
	 *            字段
	 * @param fieldName
	 *            字段名
	 * @param original
	 *            总值Map
	 */
	public void judge(String field, String fieldName, Map<String, Object> original) {
		if (!"NA".equals(field) && StringUtils.isNotBlank(field)) {
			if (original.containsKey("cnote")) {
				// 如果address中的值为"NA"，则要将其换位空字符串
				String cnote = ((String) original.get("cnote")).equals("NA") ? ""
						: (String) original.get("cnote");
				original.put("cnote", cnote + field);
			} else {
				original.put("cnote", field);
			}
		}
		original.put(fieldName, "NA");
	}

	/**
	 * 清洗Phone 与 Call
	 * 
	 * @param original
	 *            总数Map
	 * @param field
	 *            字段值
	 * @param fieldName字段名
	 */
	public void cleanPhones(Map<String, Object> original, String field, String fieldName) {
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
		String toBuffer = buffer.toString();
		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? ""
						: (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(fieldName, toBuffer);
	}

	/**
	 * @param original
	 *            总数Map
	 * @param field
	 *            字段
	 * @param Match
	 *            匹配的正则表达式
	 * @param fieldName字段名
	 */
	public void cleanField(Map<String, Object> original, String field, String match, String fieldName) {
		Pattern pat = Pattern.compile(match);
		Matcher M = pat.matcher(field);
		List<Object> listfield = new ArrayList<Object>();
		while (M.find()) {
			listfield.add(M.group()); // 将正确的数据放入集合里面
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listfield.size(); i++) {
			if (i == 0) {
				buffer.append(listfield.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listfield.get(i));
			}
		}
		String toBuffer = buffer.toString();
		String supersession = field.replaceAll(match, ""); // 将不匹配的元素取出来
		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? ""
						: (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(fieldName, toBuffer);
	}

	public Map<String, Object> changeKey(Map<String, Object> map) {
		if (map.containsKey("address")) {
			Object value = map.get("address");
			map.remove("address");
			map.put("contactAddress", value);
		}
		if (map.containsKey("homeAddress")) {
			Object value = map.get("homeAddress");
			map.remove("homeAddress");
			map.put("address", value);
		}
		if (map.containsKey("phone")) {
			Object value = map.get("phone");
			map.remove("phone");
			map.put("mobilePhone", value);
		}
		if (map.containsKey("birthday")) {
			Object value = map.get("birthday");
			map.remove("birthday");
			map.put("birthDay", value);
		}
		if (map.containsKey("rcall")) {
			Object value = map.get("rcall");
			map.remove("rcall");
			map.put("telePhone", value);
		}
		if (map.containsKey("nation")) {
			Object value = map.get("nation");
			map.remove("nation");
			map.put("nationality", value);
		}
		if (map.containsKey("transactorTel")) {
			Object value = map.get("transactorTel");
			map.remove("transactorTel");
			map.put("transactorTelePhone", value);
		}
		if (map.containsKey("companyTel")) {
			Object value = map.get("companyTel");
			map.remove("companyTel");
			map.put("companyTelePhone", value);
		}
		if (map.containsKey("zone")) {
			Object value = map.get("zone");
			map.remove("zone");
			map.put("area", value);
		}
		if (map.containsKey("name")) {
			Object value = map.get("name");
			map.put("nameAlias", value);
		}
		if (map.containsKey("transactor")) {
			Object value = map.get("transactor");
			map.remove("transactor");
			map.put("transactorNameAlias", value);
			map.put("transactorName", value);
		}
		if (map.containsKey("householdName")) {
			Object value = map.get("householdName");
			map.put("householdNameAlias", value);
		}
		return map;

	}

}
