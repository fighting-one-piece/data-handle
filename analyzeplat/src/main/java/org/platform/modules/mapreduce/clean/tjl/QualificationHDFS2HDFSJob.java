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
 * 资格考试清洗
 * 
 * @author tjl
 *
 */
public class QualificationHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return QualificationHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
//		 try {
//			 long starTime = System.currentTimeMillis();
//			 args = new
//			 String[]{"hdfs://192.168.0.115:9000/elasticsearch/work/qualification/20/records-1-m-00001",
//			 "hdfs://192.168.0.115:9000/elasticsearch_clean/work/qualification/20/records-1-m-00001"};
//			 int exitCode = ToolRunner.run(new QualificationDataCleanJob(), args);
//			 long endTime = System.currentTimeMillis();
//			 System.out.println("spend time :"+((endTime - starTime)/1000)+"S");
//			 System.exit(exitCode);
//		 } catch (Exception e) {
//			 e.printStackTrace();
//		 }

		try {
			long starTime = System.currentTimeMillis();
			String n;
			int exitCode = 0;
			for (int j = 1; j <= 2; j++) {
				for (int i = 0; i <= 9; i++) {
					if (i < 10) {
						n = "0" + i;
					} else {
						n = "" + i;
					}
					// args = new
					// String[]{"hdfs://192.168.0.115:9000/elasticsearch/tjl/ot/records-"+index+"-m-000"+n,
					// "hdfs://192.168.0.115:9000/elasticsearch/tjl/ot_out/records-"+index+"-m-000"+n};
					args = new String[] {
							"hdfs://192.168.0.10:9000/elasticsearch_original/work/qualification/20/records-" + j + "-m-000" + n,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/qualification/20/records-" + j + "-m-000"
									+ n };
					System.out.println(args[0] + ":" + args[1]);
					exitCode = ToolRunner.run(new QualificationHDFS2HDFSJob(), args);
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

class QualificationHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

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
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("email") || entry.getKey().equals("examiId")
							|| entry.getKey().equals("papersEncode") || entry.getKey().equals("applyNumber")
							|| entry.getKey().equals("papersCodes") || entry.getKey().equals("CertificationNumber")
							|| entry.getKey().equals("docId") || entry.getKey().equals("mechanismCoding"))
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
							|| entry.getKey().equals("email") || entry.getKey().equals("examiId")
							|| entry.getKey().equals("papersEncode") || entry.getKey().equals("applyNumber")
							|| entry.getKey().equals("papersCodes") || entry.getKey().equals("CertificationNumber")
							|| entry.getKey().equals("docId") || entry.getKey().equals("mechanismCoding")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("homeCall"))
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

		if (original.containsKey("homeCall")) {
			String rcall = (String) original.get("homeCall");
			if (CleanUtil.matchPhone(rcall) || CleanUtil.matchCall(rcall.replaceAll("[(]|[)]", "-"))) {
				cleanPhones(original, rcall.replaceAll("[(]|[)]", "-"), "homeCall");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("email") || entry.getKey().equals("examiId")
							|| entry.getKey().equals("papersEncode") || entry.getKey().equals("applyNumber")
							|| entry.getKey().equals("papersCodes") || entry.getKey().equals("CertificationNumber")
							|| entry.getKey().equals("docId") || entry.getKey().equals("mechanismCoding")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctRcall = ((String) (entry.getValue())).replaceAll("[(]|[)]", "-");
						Object incorrectRcall = original.get("homeCall");
						entry.setValue(incorrectRcall);
						cleanPhones(original, correctRcall, "homeCall");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(rcall, "homeCall", original);
				}
			}
		}
		
		//email
		if (original.containsKey("email")) {
			String email = (String) original.get("email");
			if (CleanUtil.matchEmail(email)) {
				cleanField(original, email, CleanUtil.emailRex, "email");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
						continue;
					if (CleanUtil.matchEmail((String) (entry.getValue()))) {
						String correctEmail = (String) entry.getValue();
						Object incorrectEmail = original.get("email");
						entry.setValue(incorrectEmail);
						cleanPhones(original, correctEmail, "email");
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
		
		if (!flag){
			if (original.containsKey("papersEncode") && CleanUtil.matchNum((String) original.get("papersEncode"))){
				flag = true;
			}else if (original.containsKey("name") && original.containsKey("address")
					&& CleanUtil.matchChinese((String) original.get("name"))
					&& CleanUtil.matchChinese((String) original.get("address")) ) {
				flag = true;
			}else if (original.containsKey("name") && original.containsKey("mailingAddress")
					&& CleanUtil.matchChinese((String) original.get("name"))
					&& CleanUtil.matchChinese((String) original.get("mailingAddress"))) {
				flag = true;
			}else if (original.containsKey("name") && original.containsKey("companyName")
					&& CleanUtil.matchChinese((String) original.get("name"))
					&& CleanUtil.matchChinese((String) original.get("companyName"))){
				flag = true;
			}
			
		}
		
		if (original.containsKey("zipCode")) {
			String zipCode =(String) original.get("zipCode");
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
	 * 
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
		if (map.containsKey("nationality")) {
			Object value = map.get("nationality");
			map.remove("nationality");
			map.put("country", value);
		}
		if (map.containsKey("nation")) {
			Object value = map.get("nation");
			map.remove("nation");
			map.put("nationality", value);
		}
		if (map.containsKey("nativePlace")) {
			Object value = map.get("nativePlace");
			map.remove("nativePlace");
			map.put("birthPlace", value);
		}
		if (map.containsKey("birthday")) {
			Object value = map.get("birthday");
			map.remove("birthday");
			map.put("birthDay", value);
		}
		if (map.containsKey("phone")) {
			Object value = map.get("phone");
			map.remove("phone");
			map.put("mobilePhone", value);
		}
		if (map.containsKey("homeCall")) {
			Object value = map.get("homeCall");
			map.remove("homeCall");
			map.put("telePhone", value);
		}
		if (map.containsKey("univercity")) {
			Object value = map.get("univercity");
			map.remove("univercity");
			map.put("university", value);
		}
		if (map.containsKey("SchoolNote")) {
			Object value = map.get("SchoolNote");
			map.remove("SchoolNote");
			map.put("schoolNote", value);
		}
		if (map.containsKey("companyName")) {
			Object value = map.get("companyName");
			map.remove("companyName");
			map.put("company", value);
		}
		if (map.containsKey("CertificateTime")) {
			Object value = map.get("CertificateTime");
			map.remove("CertificateTime");
			map.put("certificateTime", value);
		}
		if (map.containsKey("CertificationNumber")) {
			Object value = map.get("CertificationNumber");
			map.remove("CertificationNumber");
			map.put("certificationNumber", value);
		}
		if (map.containsKey("workAddress")) {
			Object value = map.get("workAddress");
			map.remove("workAddress");
			map.put("companyAddress", value);
		}
		if (map.containsKey("examiRegion")) {
			Object value = map.get("examiRegion");
			map.remove("examiRegion");
			map.put("examArea", value);
		}
		if (map.containsKey("examiId")) {
			Object value = map.get("examiId");
			map.remove("examiId");
			map.put("examId", value);
		}
		if (map.containsKey("examiMajor")) {
			Object value = map.get("examiMajor");
			map.remove("examiMajor");
			map.put("examMajor", value);
		}
		if (map.containsKey("examiDate")) {
			Object value = map.get("examiDate");
			map.remove("examiDate");
			map.put("examDate", value);
		}
		if (map.containsKey("examiLevel")) {
			Object value = map.get("examiLevel");
			map.remove("examiLevel");
			map.put("examLevel", value);
		}
		if (map.containsKey("name")) {
			Object value = map.get("name");
			map.put("nameAlias", value);
		}

		return map;
	}

}
