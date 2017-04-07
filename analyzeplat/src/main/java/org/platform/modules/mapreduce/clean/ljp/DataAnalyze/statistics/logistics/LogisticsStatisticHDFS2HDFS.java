package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics.logistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.modules.mapreduce.clean.util.DateValidation.DateUtil;

public class LogisticsStatisticHDFS2HDFS extends BaseHDFS2HDFSJob {

	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return FinancialLogisticsHDFSMapper.class;
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
//		 args = new String[] {
//		 "hdfs://192.168.0.10:9000/warehouse_original/financial/logistics/statistic_1",
//		 "hdfs://192.168.0.115:9000/user/ljpTest/input/output3"
//		 };
		String fss = args[0];
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create(fss), conf);
//			FileStatus[] fs = hdfs.listStatus(new Path(fss));
//			Path[] listPath = FileUtil.stat2Paths(fs);
			
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], null, files);
			
			int exitCode = 0;
			// metadata属性
			String line = null;
//			for (Path p : listPath) {
			for (String file : files) {
				Path p = new Path(file);
				if (!p.getName().equals("_SUCCESS") && !p.getName().equals("part-r-00000")) {
					System.out.println("传入的路径:" + p);
					line = readFirstLine(hdfs, p);
					if (line != null) {
						System.out.println("----------------->>>>>metadata属性：" + line);
						String[] args1 = new String[] { line, p.toString(), args[1] + "/" + p.getName() };
						System.out.println("执行文件--------------->>>>>>>>>>>>>" + p.getName());
						exitCode = ToolRunner.run(new LogisticsStatisticHDFS2HDFS(), args1);
					}
				}
			}

			long endTime = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(endTime - startTime);
			System.out.println("用时--------->>>" + formatter.format(date));
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
//	public static void main(String[] args) throws Exception {
//		args = new String[] {"",
//			 "hdfs://192.168.0.115:9000/user/ljpTest/input2",
//			 "hdfs://192.168.0.115:9000/user/ljpTest/input/output3"
//			 };
//		int exitCode = ToolRunner.run(new LogisticsStatisticHDFS2HDFS(), args);
//		System.exit(exitCode);
//	}

	// 根据传入的路径读取hadoop文件的第一行，并指定分隔符,返回一个字符串
	public static String readFirstLine(FileSystem hdfs, Path path) throws IOException {
		BufferedReader br = null;
		String line = null;
		try {
			FSDataInputStream fin = hdfs.open(path);
			br = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			if ((line = br.readLine()) != null) {
				return line;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				br.close();
			}
		}
		return null;
	}

	
}

class FinancialLogisticsHDFSMapper extends BaseHDFS2HDFSV1Mapper {
	static List<String> list = new ArrayList<String>();
	static {
		list.add("_id");
		list.add("insertTime");
		list.add("sourceFile");
		list.add("updateTime");
		list.add("expressId");
		list.add("orderId");
		list.add("tbExpressId");
		list.add("email");
		list.add("linkIdCard");
		list.add("linkEmail");
		list.add("bankCard");
		list.add("idCode");
		list.add("networkCode");
		list.add("rcall");
		list.add("linkCall");
		list.add("vipId");
		list.add("linkPhone");
		list.add("phone");
		list.add("idCard");
		list.add("cnote");
		list.add("endTime");
		list.add("orderDate");
		list.add("orderTime");
	}

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> incorrectMap = new HashMap<String, Object>();
		incorrectMap.putAll(original);
		int size = 5;
		// 替换空格
		Map<String, Object> map = CleanUtil.replaceSpace(original);
		// 有效字段的个数
		boolean choose = false;
		boolean flag = false;
		if (original.containsKey("phone") && original.containsKey("good")
				&& (original.containsKey("endTime") || original.containsKey("begainTime")
						|| original.containsKey("orderDate") || original.containsKey("orderTime"))
				&& original.containsKey("linkPhone") && !CleanUtil.matchNum((String) original.get("good"))) {

			if (original.containsKey("endTime") && (DateUtil.DateFromFind((String) original.get("endTime"))
					|| DateUtil.DateFromFind3((String) original.get("endTime")))) {
				choose = true;
			}

			if (original.containsKey("begainTime") && (DateUtil.DateFromFind((String) original.get("begainTime"))
					|| DateUtil.DateFromFind3((String) original.get("endTime")))) {
				choose = true;
			}

			if (original.containsKey("orderDate") && (DateUtil.DateFromFind((String) original.get("orderDate"))
					|| DateUtil.DateFromFind3((String) original.get("endTime")))) {
				choose = true;
			}

			if (original.containsKey("orderTime") && (DateUtil.DateFromFind((String) original.get("orderTime"))
					|| DateUtil.DateFromFind3((String) original.get("endTime")))) {
				choose = true;
			}

		}

		if (choose) {
			/**
			 * phone
			 */
			// phone字段清洗 phone字段中可能是电话号码也可能是座机号码
			// 判断是否包含phone字段
			String phone = (String) original.get("phone");
			// 判断是否为全角,并将全角转化为半角
			if (CleanUtil.isAllHalf(phone)) {
				phone = CleanUtil.ToDBC(phone);
			}
			// 如果字段中包含电话号码，则将电话号码提出来放入phone，剩下的值append到cnote
			if (CleanUtil.matchPhone(phone)) {
				cleanReturnField(original, phone, "phone");
				flag = true;
				// 判断是否包含座机号码，由于座机号码正则表达式区号与号码分隔符为"-"，所以这里要将其它分隔符替换为"-"
			} else {
				// 错列处理
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))) {
						String correctPhone = (String) entry.getValue();
						Object incorrectPhone = original.get("phone");
						entry.setValue(incorrectPhone);
						cleanReturnField(original, correctPhone, "phone");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}

				if (!cleanFlag) {
					judge(phone, "phone", original);
				}
			}

			/**
			 * linkPhone
			 */
			String linkPhone = (String) original.get("linkPhone");
			if (CleanUtil.isAllHalf(linkPhone)) {
				linkPhone = CleanUtil.ToDBC(linkPhone);
			}
			if (CleanUtil.matchPhone(linkPhone)) {
				cleanReturnField(original, linkPhone, "linkPhone");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (list.contains(entry.getKey()))
						continue;
					if (CleanUtil.matchPhone((String) (entry.getValue()))) {
						String correctlinkPhone = (String) entry.getValue();
						Object incorrectlinkPhone = original.get("linkPhone");
						entry.setValue(incorrectlinkPhone);
						cleanReturnField(original, correctlinkPhone, "linkPhone");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}

				if (!cleanFlag) {
					judge(linkPhone, "linkPhone", original);
				}
			}

			/** 判断快递公司与收件地址 */
			if (original.containsKey("address") || original.containsKey("city")) {
				String address = (String) original.get("address");
				String city = (String) original.get("city");
				if (address.contains("成都") || city.contains("成都")) {
					flag = true;
				}
			}
			/**
			 * 判断endTime是否是在2015-06以后
			 */
			if (original.containsKey("endTime")) {
				StringBuffer buffer = new StringBuffer();
				String newEndTime = null;
				String endTime = (String) original.get("endTime");
				if (!"".equals(endTime) && endTime != null) {
					if (DateUtil.DateFromFind(endTime)) {
//						endTime = StatisticUtil.DataFrom(endTime);
					} else if (DateUtil.DateFromFind3(endTime)) {
						String[] list = new String[3];
						int temp = 0;
						for (int i = 2; i > -1; i--) {
							if (endTime.trim().length() == 6) {
								list[i] = endTime.substring(temp, temp + 2);
								temp += 2;
							}
						}
						if (endTime.trim().length() == 5) {
							list[2] = endTime.substring(0, 2);
							list[1] = endTime.substring(2, 3);
							list[0] = endTime.substring(3, 5);
						}
						newEndTime = (buffer.append(list[0]).append(list[1]).append(list[2])).toString();
						if (!("").equals(newEndTime) && newEndTime != null) {
							if (("150601").compareTo(newEndTime) <= 0) {
								flag = true;
								size++;
							}
						}
					}
					newEndTime = endTime.substring(0, 6);
					if (!("").equals(newEndTime) && newEndTime != null) {
						if (("201506").compareTo(newEndTime) <= 0) {
							flag = true;
							size++;
						}
					}
				}
			}
			/**
			 * 判断begainTime是否是在2015-06以后
			 */
			if (original.containsKey("begainTime")) {
				StringBuffer buffer = new StringBuffer();
				String newBegainTime = null;
				String begainTime = (String) original.get("begainTime");
				if (DateUtil.DateFromFind(begainTime)) {
//					begainTime = StatisticUtil.DataFrom(begainTime);
				} else if (DateUtil.DateFromFind3(begainTime)) {
					String[] list = new String[3];
					int temp = 0;

					if (begainTime.length() == 6) {
						for (int i = 2; i > -1; i--) {
							list[i] = begainTime.substring(temp, temp + 2);
							temp += 2;
						}
					}

					if (begainTime.length() == 5) {
						list[2] = begainTime.substring(0, 2);
						list[1] = begainTime.substring(2, 3);
						list[0] = begainTime.substring(3, 5);
					}
					newBegainTime = (buffer.append(list[0]).append(list[1]).append(list[2])).toString();
					if (!("").equals(newBegainTime) && newBegainTime != null) {
						if (("150601").compareTo(newBegainTime) <= 0) {
							flag = true;
							size++;
						}
					}
				}
				newBegainTime = begainTime.substring(0, 6);
				if (!("").equals(newBegainTime) && newBegainTime != null) {
					if (("201506").compareTo(newBegainTime) <= 0) {
						flag = true;
						size++;
					}
				}

			}

			/**
			 * 判断orderDate是否是在2015-06以后
			 */
			if (original.containsKey("orderDate") && !original.containsKey("begainTime")) {
				StringBuffer buffer = new StringBuffer();
				String neworderDate = null;
				String orderDate = (String) original.get("orderDate");
				if (DateUtil.DateFromFind(orderDate)) {
//					orderDate = StatisticUtil.DataFrom(orderDate);
				} else if (DateUtil.DateFromFind3(orderDate)) {
					String[] list = new String[3];
					int temp = 0;
					for (int i = 2; i > -1; i--) {
						if (orderDate.trim().length() == 6) {
							list[i] = orderDate.substring(temp, temp + 2);
							temp += 2;
						}
					}
					if (orderDate.trim().length() == 5) {
						list[2] = orderDate.substring(0, 2);
						list[1] = orderDate.substring(2, 3);
						list[0] = orderDate.substring(3, 5);
					}
					neworderDate = (buffer.append(list[0]).append(list[1]).append(list[2])).toString();
					if (!("").equals(neworderDate) && neworderDate != null) {
						if (("150601").compareTo(neworderDate) <= 0) {
							flag = true;
							size++;
						}
					}
				}
				neworderDate = orderDate.substring(0, 6);
				if (!("").equals(neworderDate) && neworderDate != null) {
					if (("201506").compareTo(neworderDate) <= 0) {
						flag = true;
						size++;
					}
				}
			}

			/**
			 * 判断orderTime是否是在2015-06以后
			 */
			if (original.containsKey("orderTime") && !original.containsKey("begainTime")) {
				StringBuffer buffer = new StringBuffer();
				String neworderTime = null;
				String orderTime = (String) original.get("orderTime");
				if (DateUtil.DateFromFind(orderTime)) {
//					orderTime = StatisticUtil.DataFrom(orderTime);
				} else if (DateUtil.DateFromFind3(orderTime)) {
					String[] list = new String[3];
					int temp = 0;
					for (int i = 2; i > -1; i--) {
						if (orderTime.trim().length() == 6) {
							list[i] = orderTime.substring(temp, temp + 2);
							temp += 2;
						}
					}
					if (orderTime.trim().length() == 5) {
						list[2] = orderTime.substring(0, 2);
						list[1] = orderTime.substring(2, 3);
						list[0] = orderTime.substring(3, 5);
					}
					neworderTime = (buffer.append(list[0]).append(list[1]).append(list[2])).toString();
					if (!("").equals(neworderTime) && neworderTime != null) {
						if (("150601").compareTo(neworderTime) <= 0) {
							flag = true;
							size++;
						}
					}
				}
				neworderTime = orderTime.substring(0, 6);
				if (!("").equals(neworderTime) && neworderTime != null) {
					if (("201506").compareTo(neworderTime) <= 0) {
						flag = true;
						size++;
					}
				}
			}
		
			if ((original.containsKey("address") && CleanUtil.matchChinese((String) original.get("address")))
					|| (original.containsKey("name") && !CleanUtil.matchNum((String) original.get("name")))
					|| (!original.containsKey("address") && original.containsKey("linkAddress")
							&& !CleanUtil.matchNum((String) original.get("linkAddress")))
					|| (!original.containsKey("name") && original.containsKey("linkName")
							&& !CleanUtil.matchNum((String) original.get("linkName")))) {
					flag = true;
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

	}
		// 放入结果集,并替换主要字段的key
		if (flag) {
			correct.putAll(original);
		} else {
			incorrect.putAll(map);
		}
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

		for (int i = 0; i < listField.size(); i++) {
			if (i == 0) {
				buffer.append(listField.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listField.get(i));
			}
		}
		String toBuffer = buffer.toString();

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

}
