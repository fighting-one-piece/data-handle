package org.platform.modules.mapreduce.clean.skm.HDFS2HDFS.V1;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class FinancialBusinessHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	public static void main(String[] args) {
		try {
			int exitCode = 0;
			String fss;
			for (int i = 1; i <=1; i++) {
				for (int j = 0; j <= 9; j++) {
					if (j < 10) {
						fss = "hdfs://192.168.0.10:9000/elasticsearch_original/financial_new/business/20/records-" + i
								+ "-m-0000" + j;
					} else {
						fss = "hdfs://192.168.0.10:9000/elasticsearch_original/financial_new/business/20/records-" + i
								+ "-m-000" + j;
					}
					Configuration conf = new Configuration();
					FileSystem hdfs;
					hdfs = FileSystem.get(URI.create(fss), conf); // 通过uri来指定要返回的文件系统
					FileStatus[] fs = hdfs.listStatus(new Path(fss)); // FileStatus
																		// 封装了hdfs文件和目录的元数据
					Path[] listPath = FileUtil.stat2Paths(fs); // 将FileStatus对象转换成一组Path对象
					for (Path p : listPath) {
						if (!p.getName().equals("_SUCCESS") && !p.getName().equals("part-r-00000")) {

							args = new String[] { "",fss,
									"hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial_new/business/20/"
											+ p.getName() };
							exitCode = ToolRunner.run(new FinancialBusinessHDFS2HDFSJob(), args);
						}
					}
				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return BusinessHDFS2HDFSMapper.class;
	}

}

class BusinessHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		original = CleanUtil.replaceSpace(original);
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		int size = 4;
		boolean flag = false;

		// String regAddress = "^.*[\u4e00-\u9fa5]{1,}.*$";
		// 判断公司座机
		if (original.containsKey("companyCall")) {
			String companyCall = ((String) original.get("companyCall")).replaceAll("-", "").replace("(", "")
					.replace(")", "");
			if (CleanUtil.isAllHalf(companyCall)) {
				companyCall = CleanUtil.ToDBC(companyCall);
			}
			if (CleanUtil.matchCall(companyCall) || CleanUtil.matchPhone(companyCall)) {
				CleanPhone(original, companyCall, "companyCall");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("companyPhone") || entry.getKey().equals("companyFax")
							|| entry.getKey().equals("phone") || entry.getKey().equals("companyCode")
							|| entry.getKey().equals("bankId") || entry.getKey().equals("email")
							|| entry.getKey().equals("registerCapital") || entry.getKey().equals("businesTotal")
							|| entry.getKey().equals("yearBusiness") || entry.getKey().equals("totalAssets")
							|| entry.getKey().equals("wwId")|| entry.getKey().equals("note")
							|| entry.getKey().equals("setInfomation") || entry.getKey().equals("importsNum") )
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
							|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
						String m1 = (String) entry.getValue();
						String ClearCompanyCall = (String) original.get("companyCall");
						entry.setValue(ClearCompanyCall);
						CleanPhone(original, m1, "companyCall");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {
					Scrap(companyCall, "companyCall", original);
				}
			}
		}

		// 判断公司手机
		if (original.containsKey("companyPhone")) {
			String companyPhone = ((String) original.get("companyPhone")).replace("-", "").replace("(", "").replace(")",
					"");
			if (CleanUtil.isAllHalf(companyPhone)) {
				companyPhone = CleanUtil.ToDBC(companyPhone);
			}
			if (CleanUtil.matchCall(companyPhone) || CleanUtil.matchPhone(companyPhone)) {
				CleanPhone(original, companyPhone, "companyPhone");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("companyCall") || entry.getKey().equals("companyFax")
							|| entry.getKey().equals("phone") | entry.getKey().equals("companyCode")
							|| entry.getKey().equals("bankId") || entry.getKey().equals("email")
							|| entry.getKey().equals("registerCapital") || entry.getKey().equals("businesTotal")
							|| entry.getKey().equals("yearBusiness") || entry.getKey().equals("totalAssets")
							|| entry.getKey().equals("wwId")|| entry.getKey().equals("note")
							|| entry.getKey().equals("setInfomation") || entry.getKey().equals("importsNum"))
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
							|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
						String m1 = (String) entry.getValue();
						String ClearCompanyPhone = (String) original.get("companyPhone");

						entry.setValue(ClearCompanyPhone);
						CleanPhone(original, m1, "companyPhone");
						cleanFlag = true;
						size++;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					Scrap(companyPhone, "companyPhone", original);
				}
			}
		}

		// 判断公司传真
		if (original.containsKey("companyFax")) {
			String companyFax = ((String) original.get("companyFax")).replace("-", "").replace("(", "").replace(")",
					"");
			if (CleanUtil.isAllHalf(companyFax)) {
				companyFax = CleanUtil.ToDBC(companyFax);
			}
			if (CleanUtil.matchCall(companyFax)) {
				CleanFax(original, companyFax, "companyFax");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("companyCall") || entry.getKey().equals("companyPhone")
							|| entry.getKey().equals("phone") || entry.getKey().equals("companyCode")
							|| entry.getKey().equals("email") || entry.getKey().equals("bankId")
							|| entry.getKey().equals("registerCapital") || entry.getKey().equals("businesTotal")
							|| entry.getKey().equals("yearBusiness") || entry.getKey().equals("totalAssets")
							|| entry.getKey().equals("wwId")|| entry.getKey().equals("note")
							|| entry.getKey().equals("setInfomation") || entry.getKey().equals("importsNum"))
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))) {
						String m1 = (String) entry.getValue();
						String ClearCompanyFax = (String) original.get("companyFax");

						entry.setValue(ClearCompanyFax);
						CleanFax(original, m1, "companyFax");
						size++;
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					Scrap(companyFax, "companyFax", original);
				}
			}
		}
		// 判断联系人电话
		if (original.containsKey("phone")) {
			String phone = ((String) original.get("phone")).replace("-", "").replace("(", "").replace(")", "");
			if (CleanUtil.isAllHalf(phone)) {
				phone = CleanUtil.ToDBC(phone);
			}
			if (CleanUtil.matchCall(phone) || CleanUtil.matchPhone(phone)) {
				CleanPhone(original, phone, "phone");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("companyCall") || entry.getKey().equals("companyPhone")
							|| entry.getKey().equals("companyFax") || entry.getKey().equals("companyCode")
							|| entry.getKey().equals("email") || entry.getKey().equals("bankId")
							|| entry.getKey().equals("registerCapital") || entry.getKey().equals("businesTotal")
							|| entry.getKey().equals("yearBusiness") || entry.getKey().equals("totalAssets")
							|| entry.getKey().equals("wwId")|| entry.getKey().equals("note")
							|| entry.getKey().equals("setInfomation") || entry.getKey().equals("importsNum"))
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
							|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
						String m1 = (String) entry.getValue();
						String Cleanphone = (String) original.get("phone");

						entry.setValue(Cleanphone);
						CleanPhone(original, m1, "phone");

						cleanFlag = true;
						size++;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					Scrap(phone, "phone", original);
				}
			}
		}

		// 判断email
		if (original.containsKey("email")) {
			String email = (String) original.get("email");
			if (CleanUtil.matchEmail((String) original.get("email"))) {
				flag = true;
				CleanEmail(original, email, "email");
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
						continue;
					if (CleanUtil.matchEmail((String) original.get("email"))) {
						String m1 = (String) entry.getValue();
						String Newemail = (String) original.get("email");
						entry.setValue(Newemail);
						CleanEmail(original, m1, "email");

						cleanFlag = true;
						size++;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					Scrap(email, "email", original);
				}
			}
		}

		// 判断邮编
		if (original.containsKey("zipCode")) {
			String Card = ((String) original.get("zipCode")).replace("-", "").replace("(", "").replace(")", "");
			String red = "^[1-9]\\d{5}(?!\\d)$";
			if (Card.matches(red)) {
				flag = true;
			} else {
				if (StringUtils.isNotBlank(Card) && !"NA".equals(Card)) {
					if (original.containsKey("cnote")) {
						String cnote = ((String) original.get("cnote")).equals("NA") ? ""
								: (String) original.get("cnote");
						original.put("cnote", Card + cnote);
					} else {
						original.put("cnote", Card);
					}
				}
				original.remove("zipCode");
			}
		}

		// 姓名和地址同时存在
		if (original.containsKey("companyName") && original.containsKey("address")) {
			String companyName = (String) original.get("companyName");
			String address = (String) original.get("address");
			if (!("NA".equals(companyName)) && !(companyName.equals(""))&&!CleanUtil.matchNum(companyName)) {
				if (!("NA".equals(address)) && !(address.equals(""))&&CleanUtil.matchChinese(address)) {
					flag = true;
					size++;
				}
			}
		}
		// 不存在电话或座机的字段，但是有值

		for (Entry<String, Object> entry : original.entrySet()) {
			if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
					|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
					|| entry.getKey().equals("companyFax"))
				continue;
			if (!original.containsKey("phone") && !original.containsKey("companyPhone")
					&& !original.containsKey("companyCall")) {

				StringBuffer sb = new StringBuffer();
				String value = (String) entry.getValue();

				if (CleanUtil.matchPhone(value)) {

					Pattern patternPhone = Pattern.compile(CleanUtil.phoneRex);

					Matcher matcher = patternPhone.matcher(value);
					while (matcher.find()) {
						sb.append(matcher.group() + ",");
					}
					if (sb.length() > 0) {
						sb.deleteCharAt(sb.length() - 1);
					}
					original.put("companyPhone", sb.toString());

					if (entry.setValue(value.replaceAll(CleanUtil.phoneRex, "")) == null) {
						original.remove(entry.getKey());
					} else {
						entry.setValue(value.replaceAll(CleanUtil.phoneRex, ""));
					}
					size++;
					flag = true;
					break;
				}
			}
		}
		// 判断除NA之外的有效字段
		int corriSize = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			if (!"NA".equals((String) entry.getValue()) && !"".equals((String) entry.getValue())
					&& !"null".equals((String) entry.getValue()) && !"NULL".equals((String) entry.getValue())) {
				corriSize++;
			}
		}

		if (size <= 5 && corriSize <= 5) {
			flag = false;
		}
		if (flag) {
			correct.putAll(replaceKey(original));
		} else {
			incorrect.putAll(map);
		}
	}

	private Map<String, Object> replaceKey(Map<String, Object> original) {
		if (original.containsKey("companyName")) {
			original.put("company", original.get("companyName"));
			original.remove("companyName");
		}
		if (original.containsKey("companyCall")) {
			original.put("telePhone", original.get("companyCall"));
			original.remove("companyCall");
		}
		if (original.containsKey("companyPhone")) {
			original.put("mobilePhone", original.get("companyPhone"));
			original.remove("companyPhone");
		}
		if (original.containsKey("companyFax")) {
			original.put("fax", original.get("companyFax"));
			original.remove("companyFax");
		}
		if (original.containsKey("companyHead")) {
			original.put("companyHeadNameAlias", original.get("companyHead"));

		}
		if (original.containsKey("companyHead")) {
			original.put("companyHeadName", original.get("companyHead"));
			original.remove("companyHead");
		}
		if (original.containsKey("companyContact")) {
			original.put("companyContactNameAlias", original.get("companyContact"));

		}
		if (original.containsKey("companyContact")) {
			original.put("companyContactName", original.get("companyContact"));
			original.remove("companyContact");
		}
		if (original.containsKey("businesTotal")) {
			original.put("businessIncome", original.get("businesTotal"));
			original.remove("businesTotal");
		}
		if (original.containsKey("yearBusiness")) {
			original.put("yearBusinessIncome", original.get("yearBusiness"));
			original.remove("yearBusiness");
		}
		if (original.containsKey("totalAssets")) {
			original.put("totalAssetsIncome", original.get("totalAssets"));
			original.remove("totalAssets");
		}
		if (original.containsKey("phone")) {
			original.put("contactTelePhone", original.get("phone"));
			original.remove("phone");
		}
		if (original.containsKey("legal")) {
			original.put("nameAlias", original.get("legal"));
		}
		if (original.containsKey("legal")) {
			original.put("name", original.get("legal"));
			original.remove("legal");
		}
		if (original.containsKey("provinces")) {
			original.put("province", original.get("provinces"));
			original.remove("provinces");
		}
		if (original.containsKey("vallage")) {
			original.put("village", original.get("vallage"));
			original.remove("vallage");
		}
		if (original.containsKey("bankId")) {
			original.put("cardNo", original.get("bankId"));
			original.remove("bankId");
		}
		return original;
	}

	/**
	 * 
	 * @param original
	 * @param email
	 * @param string
	 */
	private void CleanEmail(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();

		Pattern pat = Pattern.compile(CleanUtil.emailRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group());
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
		String supersession = Filed.replaceAll(CleanUtil.emailRex, ""); // 将不匹配的元素取出来
		if (StringUtils.isNotBlank(supersession) && !"NA".equals(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}

		}
		original.put(FiledName, toBuffer);

	}
	
	/**
	 * 
	 * @param original
	 * @param Filed
	 *            字段值
	 * @param FiedName
	 *            字段名
	 */
	private void CleanFax(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();

		Pattern pat = Pattern.compile(CleanUtil.callRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group());
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
		String supersession = Filed.replaceAll(CleanUtil.callRex, ""); // 将不匹配的元素取出来
		      if(supersession.matches(CleanUtil.phoneRex)){
		    	    if(original.containsKey("companyPhone")){
		    	    	String fax = ((String) original.get("companyPhone")).equals("NA") ? "" : (String) original.get("companyPhone");
						original.put("companyPhone", fax + supersession);
		    	    }else{
		    	    	original.put("companyPhone", supersession);
		    	    }
		      }else{
			
		supersession = supersession.replaceAll(CleanUtil.callRex, ""); // 将不匹配的元素取出来
		
		if (StringUtils.isNotBlank(supersession) && !"NA".equals(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}

		}
		      }
		original.put(FiledName, toBuffer);

	}
	
	

	/**
	 * 
	 * @param original
	 * @param Filed
	 *            字段值
	 * @param FiedName
	 *            字段名
	 */
	private void CleanPhone(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();
		StringBuffer buf = new StringBuffer();
		Pattern pat = Pattern.compile(CleanUtil.phoneRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group());
		}
		String supersession = Filed.replaceAll(CleanUtil.phoneRex, ""); // 将不匹配的元素取出来

		pat = Pattern.compile(CleanUtil.callRex);
		M = pat.matcher(supersession);
		while (M.find()) {
			listFiled.add(M.group());
		}

		for (int i = 0; i < listFiled.size(); i++) {
			if (i == 0) {
				buf.append(listFiled.get(i));
			} else {
				buf.append("," + listFiled.get(i));
			}
		}
		String toBuffer = buf.toString();
		supersession = supersession.replaceAll(CleanUtil.callRex, ""); // 将不匹配的元素取出来
		if (StringUtils.isNotBlank(supersession) && !"NA".equals(supersession)) {

			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}

		}
		original.put(FiledName, toBuffer);
	}

	/*
	 * 将废值替换为NA
	 */
	public void Scrap(String Filed, String FiledName, Map<String, Object> original) {
		if (!"NA".equals(Filed) && StringUtils.isNotBlank(Filed)) {

			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + Filed);
			} else {
				original.put("cnote", Filed);
			}
		}
		original.put(FiledName, "NA");
	}

}