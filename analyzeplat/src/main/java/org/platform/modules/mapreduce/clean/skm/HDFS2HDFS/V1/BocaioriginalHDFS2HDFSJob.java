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

public class BocaioriginalHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	public static void main(String[] args) {
		try {
			int exitCode = 0;
			String fss;
			// for (int i = 1; i <=1; i++) {
			// for (int j = 0; j <= 11; j++) {
			// if (j < 10) {
			// fss =
			// "hdfs://192.168.0.10:9000/elasticsearch_original/financial_new/business/20/records-"
			// + i
			// + "-m-0000" + j;
			// } else {
			// fss =
			// "hdfs://192.168.0.10:9000/elasticsearch_original/financial_new/business/20/records-"
			// + i
			// + "-m-000" + j;
			// }
			fss = "hdfs://192.168.0.115:9000/skm/";
			Configuration conf = new Configuration();
			FileSystem hdfs;
			hdfs = FileSystem.get(URI.create(fss), conf); // 通过uri来指定要返回的文件系统
			FileStatus[] fs = hdfs.listStatus(new Path(fss)); // FileStatus
																// 封装了hdfs文件和目录的元数据
			Path[] listPath = FileUtil.stat2Paths(fs); // 将FileStatus对象转换成一组Path对象
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS") && !p.getName().equals("part-r-00000")) {

					args = new String[] { "", fss, "hdfs://192.168.0.115:9000/skm1/" + p.getName() };
					exitCode = ToolRunner.run(new BocaioriginalHDFS2HDFSJob(), args);
				}
				// }
				// }
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return BocaioriginalHDFS2HDFSMapper.class;
	}

}

class BocaioriginalHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		original = CleanUtil.replaceSpace(original);
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		int size = 4;
		boolean flag = false;

		// 判断价格
		
			// 判断手机座机
			if (original.containsKey("mobilePhone")) {
				String phone = ((String) original.get("mobilePhone")).replaceAll("-", "").replace("(", "").replace(")",
						"");
				if (CleanUtil.isAllHalf(phone)) {
					phone = CleanUtil.ToDBC(phone);
				}
				if (CleanUtil.matchPhone(phone)) {
					CleanPhone(original, phone, "mobilePhone");
					flag = true;
				} else {
					boolean cleanFlag = false;
					for (Entry<String, Object> entry : original.entrySet()) {
						if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
								|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
								|| entry.getKey().equals("idCard") || entry.getKey().equals("cardNo"))
							continue;
						if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
								|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
							String m1 = (String) entry.getValue();
							String ClearCompanyCall = (String) original.get("mobilePhone");
							entry.setValue(ClearCompanyCall);
							CleanPhone(original, m1, "mobilePhone");
							cleanFlag = true;
							flag = true;
							size++;
							break;
						}
					}
					if (!cleanFlag) {
						Scrap(phone, "mobilePhone", original);
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


		// 判断除NA之外的有效字段
		int corriSize = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			if (!"NA".equals((String) entry.getValue()) && !"".equals((String) entry.getValue())
					&& !"null".equals((String) entry.getValue()) && !"NULL".equals((String) entry.getValue())) {
				corriSize++;
			}
		}

		if (size <= 4 && corriSize <= 5) {
			flag = false;
		}
		if (flag) {
			correct.putAll(original);
		} else {
			incorrect.putAll(map);
		}
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