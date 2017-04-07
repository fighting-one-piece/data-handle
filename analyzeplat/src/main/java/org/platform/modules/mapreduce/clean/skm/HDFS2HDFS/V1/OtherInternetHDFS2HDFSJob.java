package org.platform.modules.mapreduce.clean.skm.HDFS2HDFS.V1;

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

public class OtherInternetHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	public static void main(String[] args) {

		// try {
		// int exitCode = ToolRunner.run(new CleanOtherInternet(), args);
		// System.exit(exitCode);
		// } catch (Exception e) {
		//
		// e.printStackTrace();
		// }
		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;

			int index = 0;
			for (index = 1; index <= 3; index++) {
				for (int i = 0; i <= 9; i++) {
					if (i < 10) {
						args = new String[] {
								"hdfs://192.168.0.10:9000/elasticsearch_original/other/interne/20/records-" + index
										+ "-m-0000" + i,
								"hdfs://192.168.0.10:9000/elasticsearch_clean_1/other/internet/20/records-" + index
										+ "-m-0000" + i };
					}
					exitCode = ToolRunner.run(new OtherInternetHDFS2HDFSJob(), args);
				}
			}

			/**
			 * args = new String[] {
			 * "hdfs://192.168.0.115:9000/elasticsearch/operator/telecom/",
			 * "hdfs://192.168.0.115:9000/elasticsearch/operator/telecom_out" };
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

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return InternetHDFS2HDFSMapper.class;
	}

}

class InternetHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		original = CleanUtil.replaceSpace(original);
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);

		int size = 5;
		boolean flag = false;
		// String regAddress = "^.*[\u4e00-\u9fa5]{1,}.*$";

		// account password

		// 判断电话
		if (original.containsKey("phone")) {
			String Phone = ((String) original.get("phone")).replace("-", "").replace("(", "").replace(")", "");
			if (CleanUtil.matchCall(Phone) || CleanUtil.matchPhone(Phone)) {
				CleanPhone(original, Phone, "phone");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("homeCall") || entry.getKey().equals("password"))
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
							|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))) {
						String m1 = (String) entry.getValue();
						String ClearPhone = (String) original.get("phone");

						entry.setValue(ClearPhone);
						CleanPhone(original, m1, "phone");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(Phone, "phone", original);
				}
			}
		}

		// 判断住址电话
		if (original.containsKey("homeCall")) {
			String homeCall = ((String) original.get("homeCall"));
			if (CleanUtil.matchCall(homeCall) || CleanUtil.matchPhone(homeCall)) {
				CleanPhone(original, homeCall, "homeCall");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("phone") || entry.getKey().equals("password"))
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
							|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))) {
						String m1 = (String) entry.getValue();
						String ClearhomeCall = (String) original.get("homeCall");

						entry.setValue(ClearhomeCall);
						CleanPhone(original, m1, "homeCall");

						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(homeCall, "homeCall", original);
				}
			}
		}
		 if(!flag){
		if (original.containsKey("account") && original.containsKey("password")) {
			String account = (String) original.get("account");
			String password = (String) original.get("password");
			if (!("NA".equals(password)) || !(password.equals(""))) {
				if (!("NA".equals(account)) || !(account.equals(""))) {
					flag = true;
				}
			}

		}
		}
		 if(!flag){
		if (original.containsKey("name") && original.containsKey("nativePlace")) {
			String account = (String) original.get("name");
			String password = (String) original.get("nativePlace");
			if (!("NA".equals(password)) || !(password.equals(""))) {
				if (!("NA".equals(account)) || !(account.equals(""))) {
					flag = true;
				}
			}

		}
		
		 }
		
		
		//判断除NA之外的有效字段
				int corriSize = 0;
				for(Entry<String, Object> entry : original.entrySet()){
					if(!"NA".equals((String)entry.getValue())){
						corriSize++;
					}
				}
		if (original.size() <= size||corriSize<5) {
			flag = false;
		}
		if (flag) {
			correct.putAll(replaceKey(original));
		} else {
			incorrect.putAll(map);
		}
	}

	private Map<String, Object> replaceKey(Map<String, Object> original) {

		if (original.containsKey("name")) {
			original.put("nameAlias", original.get("name"));
		}

		if (original.containsKey("nativePlace")) {
			original.put("birthPlace", original.get("nativePlace"));
			original.remove("nativePlace");
		}
		if (original.containsKey("phone")) {
			original.put("mobilePhone", original.get("phone"));
			original.remove("phone");
		}
		if (original.containsKey("homeCall")) {
			original.put("telePhone", original.get("homeCall"));
			original.remove("homeCall");
		}
		if (original.containsKey("notes")) {
			original.put("note", original.get("notes"));
			original.remove("notes");
		}

		return original;
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

		if (StringUtils.isNotBlank(supersession)) {
			
			if(original.containsKey("cnote")){
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote+supersession);
			}else{
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
			  if(original.containsKey("cnote")){
				  String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
					original.put("cnote", cnote + Filed);
			  }else{
				  original.put("cnote", Filed);
			  }
		}
		original.put(FiledName, "NA");
	}

}
