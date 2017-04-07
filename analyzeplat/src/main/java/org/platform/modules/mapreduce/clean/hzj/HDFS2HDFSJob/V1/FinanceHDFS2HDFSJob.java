package org.platform.modules.mapreduce.clean.hzj.HDFS2HDFSJob.V1;

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

public class FinanceHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return FinanceHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;
			for (int j = 8; j <=8; j++) {
				for (int i = 4; i <= 9; i++) {
				args = new String[] { "hdfs://192.168.0.10:9000/elasticsearch_original/financial_new/finance/20/records-"+j+"-m-0000"+i,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial_new/finance/20/records-"+j+"-m-0000"+i};
					exitCode = ToolRunner.run(new FinanceHDFS2HDFSJob(), args);
				}
				
			}
			//Configuration conf = new Configuration();
			/*if (args.length != 2) {
				System.err.println("error:please write two path,input and output");
			}*/
		/*	Path path = new Path(args[1]);
			FileSystem file = path.getFileSystem(conf);
			if (file.exists(path)) {
				file.delete(path, true);
			}*/
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

class FinanceHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {

		int size = 5;
		boolean flag = false;
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);

		if (original.containsKey("idCard")) {
			String idCard = ((String) original.get("idCard"));
			if (CleanUtil.matchIdCard(idCard)) {
				cleanFiled(original, idCard, CleanUtil.idCardRex, "idCard");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("companyPhone"))
						continue;
					if (CleanUtil.matchIdCard((String) (entry.getValue()))) {
						String correctIdCard = (String) entry.getValue();
						Object incorrectIdCard = original.get("idCard");
						entry.setValue(incorrectIdCard);
						cleanFiled(original, correctIdCard, CleanUtil.idCardRex, "idCard");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(idCard, "idCard", original);
				}
			}

		}
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			// 如果字段中包含电话号码，则将电话号码提出来放入phone，剩下的值append到address
			if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone.replaceAll("[(]|[)]", "-"))) {
				cleanReturnFiled(original, phone, "phone");
				flag = true;
				// 判断是否包含座机号码，由于座机号码正则表达式区号与号码分隔符为"-"，所以这里要将其它分隔符替换为"-"
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("homeCall")
							|| entry.getKey().equals("companyPhone")|| entry.getKey().equals("email"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("phone");
						entry.setValue(incorrectPhone);
						cleanReturnFiled(original, correctPhone, "phone");
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
			String homeCall = (String) original.get("homeCall");
			if(CleanUtil.isAllHalf(homeCall)){
				homeCall = CleanUtil.ToDBC(homeCall);
			}
			if (CleanUtil.matchPhone(homeCall) || CleanUtil.matchCall(homeCall.replaceAll("[(]|[)]", "-"))) {
				cleanReturnFiled(original, homeCall, "homeCall");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("companyPhone")|| entry.getKey().equals("email"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctcall = (String) entry.getValue();
						Object incorrectcall = original.get("homeCall");
						entry.setValue(incorrectcall);
						cleanReturnFiled(original, correctcall, "homeCall");
						cleanFlag = true;
						flag = true;
						break;
					}
				}

				if (!cleanFlag) {
					size++;
					judge(homeCall, "homeCall", original);
				}
			}
		}
		if (original.containsKey("companyPhone")) {
			String companyPhone = (String) original.get("companyPhone");
			if (CleanUtil.matchPhone(companyPhone) || CleanUtil.matchCall(companyPhone.replaceAll("[(]|[)]", "-"))) {
				cleanReturnFiled(original, companyPhone, "companyPhone");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard") || entry.getKey().equals("phone")
							|| entry.getKey().equals("homeCall")||entry.getKey().equals("email"))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))
							|| CleanUtil.matchCall(((String) (entry.getValue())).replaceAll("[(]|[)]", "-"))) {
						String correctcall = (String) entry.getValue();
						Object incorrectcall = original.get("companyPhone");
						entry.setValue(incorrectcall);
						cleanReturnFiled(original, correctcall, "companyPhone");
						cleanFlag = true;
						flag = true;
						break;
					}
				}

				if (!cleanFlag) {
					size++;
					judge(companyPhone, "companyPhone", original);
				}
			}
		}
		
		if (original.containsKey("email")) {
			String email = ((String) original.get("email"));
			if (CleanUtil.matchEmail(email)) {
				cleanFiled(original, email, CleanUtil.emailRex, "email");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("companyPhone")||entry.getKey().equals("idCard")
							||entry.getKey().equals("homeCall")||entry.getKey().equals("phone"))
						continue;
					if (CleanUtil.matchEmail((String) (entry.getValue()))) {
						String correctEmail = (String) entry.getValue();
						Object incorrectEmail = original.get("email");
						entry.setValue(incorrectEmail);
						cleanFiled(original, correctEmail, CleanUtil.emailRex, "email");
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
		if (original.containsKey("zipCode")) {
			String zipCode =(String) original.get("zipCode");
			if (!CleanUtil.matchNum(zipCode)){
				judge(zipCode, "zipCode", original);
			}
					
		}
        int corriSize = 0;
        for (Entry<String,Object> entry:original.entrySet()) {
			if(!("NA".equals(entry.getValue()))){
				corriSize++;
			}
		}
		if(!flag){
			 if(original.containsKey("name") && original.containsKey("address")
						&& !CleanUtil.matchNum((String) original.get("name"))
						&& CleanUtil.matchChinese((String) original.get("address"))){
				 flag =true;
			 }else if(original.containsKey("companyName") && original.containsKey("companyAddress")
						&& CleanUtil.matchChinese((String) original.get("companyName"))
						&& CleanUtil.matchChinese((String) original.get("companyAddress"))){
				 flag =true;
			 }
		}
		if (original.size() <= size||corriSize < 5) {
			flag = false;
		}
		original = CleanUtil.replaceSpace(original);
		if (flag) {
			correct.putAll(changeKey(original));
		} else {
			incorrect.putAll(original);

		}

	}

	public void cleanFiled(Map<String, Object> original, String Filed, String Match, String FiledName) {
		Pattern pat = Pattern.compile(Match);
		Matcher M = pat.matcher(Filed);
		List<Object> listFiled = new ArrayList<Object>();
		while (M.find()) {
			listFiled.add(M.group()); // 将正确的数据放入集合里面
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listFiled.size(); i++) {
			if (i == 0) {
				buffer.append(listFiled.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listFiled.get(i));
			}
		}
		String toBuffer = buffer.toString();
		String supersession = Filed.replaceAll(Match, ""); // 将不匹配的元素取出来
		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? ""
						: (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(FiledName, toBuffer);
	}

	/**
	 * 清洗Phone 与 Call
	 * 
	 * @param original
	 *            总数Mapip
	 * @param Filed
	 *            字段
	 * @param Match
	 *            匹配的正则表达式
	 * @param FiledName字段名
	 */
	public void cleanReturnFiled(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();
		StringBuffer buffer = new StringBuffer();

		Pattern pat = Pattern.compile(CleanUtil.phoneRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group()); // 将正确的数据放入集合里面
		}

		String supersession = Filed.replaceAll(CleanUtil.phoneRex, ""); // 将不匹配的元素取出来

		pat = Pattern.compile(CleanUtil.callRex);
		M = pat.matcher(supersession);
		while (M.find()) {
			listFiled.add(M.group()); // 将正确的数据放入集合里面
		}
		for (int i = 0; i < listFiled.size(); i++) {
			if (i == 0) {
				buffer.append(listFiled.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listFiled.get(i));
			}
		}
		String toBuffer = buffer.toString();
		supersession = supersession.replaceAll(CleanUtil.callRex, ""); // 将不匹配的元素取出来

		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? ""
						: (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(FiledName, toBuffer);
	}

	/**
	 * 将废值替换为NA
	 * 
	 * @param Filed
	 *            字段
	 * @param FiledName
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

	public Map<String, Object> changeKey(Map<String, Object> map) {
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
		if (map.containsKey("companyPhone")) {
			Object value = map.get("companyPhone");
			map.remove("companyPhone");
			map.put("companyMobilePhone", value);
		}
		if (map.containsKey("cardNumber")) {
			Object value = map.get("cardNumber");
			map.remove("cardNumber");
			map.put("cardNo", value);
		}
		if (map.containsKey("name")) {
			Object value = map.get("name");
			map.put("nameAlias", value);
		}
		return map;
	}

}
