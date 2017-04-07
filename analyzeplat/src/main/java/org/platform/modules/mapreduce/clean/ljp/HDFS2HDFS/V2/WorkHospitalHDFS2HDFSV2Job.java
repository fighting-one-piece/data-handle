package org.platform.modules.mapreduce.clean.ljp.HDFS2HDFS.V2;

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
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV2Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
/**
 * 清洗规则V2,分隔符式新版清洗 修改参数: 1.输入路径 2.输出路径 3.循环数的更改
 * 
 * @author Administrator
 *
 */
public class WorkHospitalHDFS2HDFSV2Job extends BaseHDFS2HDFSJob {
	public Class<? extends BaseHDFS2HDFSV2Mapper> getMapperClass() {
		return HospitalHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;

			for (int i = 0; i <= 9; i++) {
				args = new String[] {
						"hdfs://192.168.0.10:9000/elasticsearch_original/work/hospital/20/records-1-m-0000" + i,
						"hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/hospital/20/records-1-m-0000" + i };
				exitCode = ToolRunner.run(new WorkHospitalHDFS2HDFSV2Job(), args);
			}

			/*
			 * args = new String[] {
			 * "hdfs://192.168.0.115:9000/elasticsearch/work/hospital/",
			 * "hdfs://192.168.0.115:9000/elasticsearch/work/hospital_out/" };
			 */

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

class HospitalHDFS2HDFSMapper extends BaseHDFS2HDFSV2Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {

		int size = 5;
		boolean flag = false;
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		/*
		 * idCard
		 */
		if (original.containsKey("idCard")) {
			String idCard = ((String) original.get("idCard"));
			if (CleanUtil.matchIdCard(idCard)) {
				cleanField(original, idCard, CleanUtil.idCardRex, "idCard");
				flag = true;
			} else {// 如果不包含身份证号则可能错列
				boolean cleanFlag = false;
				// 遍历map，跳过特殊字段，判断是否有包含idCard的字段
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idNumber") || entry.getKey().equals("fileNumber")
							|| entry.getKey().equals("certificateNumber") || entry.getKey().equals("email"))
						continue;
					// 如果有包含idCard的字段，则将其与idCard字段值进行交换
					if (CleanUtil.matchIdCard((String) (entry.getValue()))) {
						String correctIdCard = (String) entry.getValue();
						Object incorrectIdCard = original.get("idCard");
						entry.setValue(incorrectIdCard);
						// 身份证号提取出来放入idCard，提取后剩下的数据append到cnote上
						cleanField(original, correctIdCard, CleanUtil.idCardRex, "idCard");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				// 如果没有找到身份证号，则idCard字段为废值，将其值append到cnote上，并将idCard字段设为"NA"
				if (!cleanFlag) {
					size++;
					judge(idCard, "idCard", original);
				}
			}

		}

		/*
		 * idNumber
		 */
		if (original.containsKey("idType") && ((String) original.get("idType")).contains("身份证")) {
			if (original.containsKey("idNumber")) {
				String idNumber = ((String) original.get("idNumber"));
				if (CleanUtil.matchIdCard(idNumber)) {
					cleanField(original, idNumber, CleanUtil.idCardRex, "idNumber");
					flag = true;
				} else {
					boolean cleanFlag = false;
					for (Entry<String, Object> entry : original.entrySet()) {
						if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
								|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
								|| entry.getKey().equals("idCard") || entry.getKey().equals("fileNumber")
								|| entry.getKey().equals("certificateNumber") || entry.getKey().equals("email"))
							continue;
						if (CleanUtil.matchIdCard((String) (entry.getValue()))) {
							String correctIdNumber = (String) entry.getValue();
							Object incorrectIdNumber = original.get("idNumber");
							entry.setValue(incorrectIdNumber);
							cleanField(original, correctIdNumber, CleanUtil.idCardRex, "idNumber");
							cleanFlag = true;
							flag = true;
							break;
						}
					}
					if (!cleanFlag) {
						size++;
						judge(idNumber, "idNumber", original);
					}
				}
			}
		}

		// 将所有的字段去空格
		CleanUtil.replaceSpace(original);

		/**
		 * phone
		 */

		// phone字段清洗 phone字段中可能是电话号码也可能是座机号码
		// 判断是否包含phone字段
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			// 判断是否为全角,并将全角转化为半角
			if (CleanUtil.isAllHalf(phone)) {
				phone = CleanUtil.ToDBC(phone);
			}
			// 如果字段中包含电话号码，则将电话号码提出来放入phone，剩下的值append到address
			if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, phone, "phone");
				flag = true;
				// 判断是否包含座机号码，由于座机号码正则表达式区号与号码分隔符为"-"，所以这里要将其它分隔符替换为"-"
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("rcall")
							|| entry.getKey().equals("idNumber") || entry.getKey().equals("fileNumber")
							|| entry.getKey().equals("certificateNumber") || entry.getKey().equals("email")
							|| entry.getKey().equals("fax"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("phone");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "phone");
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

		/**
		 * rCall
		 */
		if (original.containsKey("rcall")) {
			String rcall = (String) original.get("rcall");
			if (CleanUtil.isAllHalf(rcall)) {
				rcall = CleanUtil.ToDBC(rcall);
			}
			if (CleanUtil.matchPhone(rcall) || CleanUtil.matchCall(rcall.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, rcall, "rcall");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("idNumber") || entry.getKey().equals("fileNumber")
							|| entry.getKey().equals("certificateNumber") || entry.getKey().equals("email")
							|| entry.getKey().equals("fax"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctRcall = (String) entry.getValue();
						Object incorrectRcall = original.get("rcall");
						entry.setValue(incorrectRcall);
						cleanReturnField(original, correctRcall, "rcall");
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

		/**
		 * FAX
		 */
		if (original.containsKey("fax")) {
			String fax = (String) original.get("fax");
			if (CleanUtil.matchCall(fax.replaceAll("[(]|[)]", "-"))) {
				cleanReturnField(original, fax, "fax");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("idNumber") || entry.getKey().equals("fileNumber")
							|| entry.getKey().equals("certificateNumber") || entry.getKey().equals("email")
							|| entry.getKey().equals("rcall") || entry.getKey().equals("address"))
						continue;
					if (CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctFax = (String) entry.getValue();
						Object incorrectFax = original.get("fax");
						entry.setValue(incorrectFax);
						cleanReturnField(original, correctFax, "fax");
						cleanFlag = true;
						flag = true;
						break;
					}
				}

				if (!cleanFlag) {
					size++;
					judge(fax, "fax", original);
				}
			}
		}

		/*
		 * email
		 */
		if (original.containsKey("email")) {
			String email = (String) original.get("email");
			if (CleanUtil.matchEmail(email)) {
				cleanReturnField(original, email, "email");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("rcall") || entry.getKey().equals("fax"))
						continue;
					if (CleanUtil.matchEmail((String) (entry.getValue()))) {
						String correctEmail = (String) entry.getValue();
						Object incorrectEmail = original.get("email");
						entry.setValue(incorrectEmail);
						cleanReturnField(original, correctEmail, "email");
						cleanFlag = true;
						flag = true;
						break;
					}
				}

				if (!cleanFlag) {
					size++;
					judge(email, "email", original);
				}
			}
		}

		/**
		 * 判断合格的姓名与地址,与合格的证件类型与证件号
		 */
		if (!flag) {
			if ((original.containsKey("name"))
					&& (original.containsKey("address") || original.containsKey("graduateSchool")
							|| original.containsKey("company"))
					&& !CleanUtil.matchNum((String) original.get("name"))
					&& (CleanUtil.matchChinese((String) original.get("address"))
							|| (CleanUtil.matchChinese((String) original.get("graduateSchool"))
									|| CleanUtil.matchNum((String) original.get("graduateSchool"))
									|| CleanUtil.matchChinese((String) original.get("company"))))) {
				flag = true;
			} else if ((original.containsKey("idType")) && original.containsKey("idNumber")
					&& (!(((String) original.get("idType")).contains("身份证")))
					&& !CleanUtil.matchNum((String) original.get("idType"))
					&& (CleanUtil.matchChinese((String) original.get("idNumber"))
							|| CleanUtil.matchNum((String) original.get("idNumber")))) {
				flag = true;
			} else if (original.containsKey("certificateNumber")
					&& CleanUtil.matchNum((String) original.get("certificateNumber"))) {
				flag = true;
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
		if (original.size() <= size || corriSize <= 5) {
			flag = false;
		}

		// 放入结果集
		if (flag) {
			correct.putAll(keyReplace(original));
		} else {
			incorrect.putAll(map);
		}
	}

	/**
	 * 清洗idCard 与 idNumber
	 * 
	 * @param original
	 *            总数Map
	 * @param field
	 *            字段
	 * @param Match
	 *            匹配的正则表达式
	 * @param fieldName字段名
	 */
	public void cleanField(Map<String, Object> original, String field, String Match, String fieldName) {
		Pattern pat = Pattern.compile(Match);
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
		String supersession = field.replaceAll(Match, ""); // 将不匹配的元素取出来
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
			listField.add(M.group()); // 将正确的数据放入集合里面
		}

		String supersession = field.replaceAll(CleanUtil.phoneRex, ""); // 将不匹配的元素取出来

		pat = Pattern.compile(CleanUtil.callRex);
		M = pat.matcher(supersession);
		while (M.find()) {
			listField.add(M.group()); // 将正确的数据放入集合里面
		}
		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		String toBuffer = buffer.toString();
		supersession = supersession.replaceAll(CleanUtil.callRex, ""); // 将不匹配的元素取出来

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
				// 如果cnote中的值为"NA"，则要将其换位空字符串
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + field);
			} else {
				original.put("cnote", field);
			}
		}
		original.put(fieldName, "NA");
	}

	/**
	 * 清洗后替换主要字段的key
	 * 
	 * @param original
	 * @return
	 */
	public Map<String, Object> keyReplace(Map<String, Object> original) {
		if (original.containsKey("phone")) {
			original.put("mobilePhone", original.get("phone"));
			original.remove("phone");
		}
		if (original.containsKey("rcall")) {
			original.put("telePhone", original.get("rcall"));
			original.remove("rcall");
		}
		if (original.containsKey("birthday")) {
			original.put("birthDay", original.get("birthday"));
			original.remove("birthday");
		}
		if (original.containsKey("nationality")) {
			original.put("country", original.get("nationality"));
			original.remove("nationality");
		}
		if (original.containsKey("familyName")) {
			original.put("nationality", original.get("familyName"));
			original.remove("familyName");
		}
		if (original.containsKey("graduateSchool")) {
			original.put("university", original.get("graduateSchool"));
			original.remove("graduateSchool");
		}
		if (original.containsKey("graduateSpecialty")) {
			original.put("major", original.get("graduateSpecialty"));
			original.remove("graduateSpecialty");
		}
		if (original.containsKey("graduateRemarks")) {
			original.put("graduateNote", original.get("graduateRemarks"));
			original.remove("graduateRemarks");
		}
		if (original.containsKey("schoolRemark")) {
			original.put("schoolNote", original.get("schoolRemark"));
			original.remove("schoolRemark");
		}
		if (original.containsKey("examSpecialty")) {
			original.put("examMajor", original.get("examSpecialty"));
			original.remove("examSpecialty");
		}
		if (original.containsKey("name")) {
			original.put("nameAlias", original.get("name"));
		}

		return original;
	}
}
