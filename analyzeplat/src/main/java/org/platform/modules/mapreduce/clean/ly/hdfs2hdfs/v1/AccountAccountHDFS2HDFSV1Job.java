package org.platform.modules.mapreduce.clean.ly.hdfs2hdfs.v1;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

/**
 * 账号数据清洗
 * 
 * @author ly
 *
 */
public class AccountAccountHDFS2HDFSV1Job extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return AccountHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		 args = new
		 String[]{"","hdfs://192.168.0.115:9000/elasticsearch/account","hdfs://192.168.0.115:9000/ly"};
		long startTime = System.currentTimeMillis();
		if (args.length != 3) {
			System.out.println("参数不正确");
			System.exit(2);
		}
		String fss = args[1];
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			String[] args1;
			hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			int exitCode = 0;
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS") && !"part-r-00000".equals(p.getName())) {
					args1 = new String[] { args[0], p.toString(), args[2] + "/" + p.getName() };
					System.out.println("执行文件--------------->>>>>>>>>>>>>" + p.toString());
					exitCode = ToolRunner.run(new AccountAccountHDFS2HDFSV1Job(), args1);
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

}

class AccountHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	// 存在主要字段，但是不满足条件的value值
	static List<String> erroList = new ArrayList<String>();
	static {
		erroList.add("NA");
		erroList.add("NULL");
		erroList.add("");
		erroList.add("null");
	}

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		// 满足主要字段的个数
		int sum = 0;

		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		// 替换空白字符
		CleanUtil.replaceSpace(original);
		// 特殊情况：如果一条记录里面有邮箱有密码，但是没有账号字段，则将邮箱字段放入账号字段
		if (original.containsKey("email") && original.containsKey("password") && !original.containsKey("account")) {
			String email = (String) original.get("email");
			original.put("account", email);
			original.remove("email");
		}

		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			if (CleanUtil.matchIdCard(idCard)) {
				cleanField(original, idCard, CleanUtil.idCardRex, "idCard");
				sum++;
			} else {
				original.put("idCard", "NA");
				// 判断是否添加cnote字段
				if (!erroList.contains(idCard)) {
					if (original.containsKey("cnote")) {
						String cnote = (String) original.get("cnote");
						original.put("cnote", cnote + idCard);
					} else {
						original.put("cnote", idCard);
					}
				}
			}
		}

		// 对email字段进行清洗，不正确直接设为NA
		if (original.containsKey("email")) {
			String email = (String) original.get("email");
			if (CleanUtil.matchEmail(email)) {
				cleanField(original, email, CleanUtil.emailRex, "email");
				sum++;
			} else {
				original.put("email", "NA");
				// 判断是否添加cnote字段
				if (!erroList.contains(email)) {
					if (original.containsKey("cnote")) {
						String cnote = ((String) original.get("cnote")).equals("NA") ? ""
								: (String) original.get("cnote");
						original.put("cnote", cnote + email);
					} else {
						original.put("cnote", email);
					}
				}
			}
		}

		// 对phone字段进行清洗，不正确直接设为NA
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone.replaceAll("[(]|[)]", "-"))) {
				cleanPhone(original, phone.replaceAll("[(]|[)]", "-"));
				sum++;
			} else {
				original.put("phone", "NA");
				// 判断是否添加cnote字段
				if (!erroList.contains(phone)) {
					if (original.containsKey("cnote")) {
						String cnote = ((String) original.get("cnote")).equals("NA") ? ""
								: (String) original.get("cnote");
						original.put("cnote", cnote + phone);
					} else {
						original.put("cnote", phone);
					}
				}
			}
		}
		/*
		 * 判断标示性字段同时存在的情况
		 */
		boolean flag = false;
		// 1.判断账号和密码同时存在的情况
		if (sum == 0 && original.containsKey("account") && original.containsKey("password")) {
			String account = (String) original.get("account");
			String password = (String) original.get("password");
			if (!erroList.contains(account) && !erroList.contains(password)) {
				flag = true;
			}
		}
		// 2.判断用户名和密码同时存在的情况
		if (!flag && original.containsKey("userName") && original.containsKey("password")) {
			String userName = (String) original.get("userName");
			String password = (String) original.get("password");
			if (!erroList.contains(userName) && !erroList.contains(password)) {
				flag = true;
			}
		}
		// 3.判断姓名和密码同时存在的情况
		if (!flag && original.containsKey("name") && original.containsKey("password")) {
			String name = (String) original.get("name");
			String password = (String) original.get("password");
			if (!erroList.contains(name) && !erroList.contains(password)) {
				flag = true;
			}
		}
		// 4.判断用户名和加密的内容（encrypt）同时存在的情况
		if (!flag && original.containsKey("userName") && original.containsKey("encrypt")) {
			String userName = (String) original.get("userName");
			String encrypt = (String) original.get("encrypt");
			if (!erroList.contains(userName) && !erroList.contains(encrypt)) {
				flag = true;
			}
		}
		// 5.判断账号和加密的内容（encrypt）同时存在的情况
		if (!flag && original.containsKey("account") && original.containsKey("encrypt")) {
			String account = (String) original.get("account");
			String encrypt = (String) original.get("encrypt");
			if (!erroList.contains(account) && !erroList.contains(encrypt)) {
				flag = true;
			}
		}

		// 判断有效字段个数
		int times = 0;
		// 每条记录特有的字段
		int size = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			if (!erroList.contains((String) entry.getValue())) {
				times++;
			}
			if ("insertTime".equals((String) entry.getKey()) || "_id".equals((String) entry.getKey())
					|| "sourceFile".equals((String) entry.getKey()) || "updateTime".equals((String) entry.getKey())) {
				size++;
			}
		}

		if ((sum > 0 || flag) && times > size + 1) {
			correct.putAll(changeKey(original));
		} else {
			String spaceRegex = "\\s+|(----){1}|(---){1}|(-----){1}";
			if (original.containsKey("password") && original.containsKey("email")) {
				String email = (String) original.get("email");
				String password = (String) original.get("password");
				String[] str = email.split(spaceRegex);
				if (str.length == 2 && erroList.contains(password) && !erroList.contains(str[1])) {
					original.put("email", str[0]);
					original.put("password", str[1]);
					correct.putAll(changeKey(original));
					return;
				}
			}
			if (original.containsKey("password") && original.containsKey("account")) {
				String account = (String) original.get("account");
				String password = (String) original.get("password");
				String[] str = account.split(spaceRegex);
				if (str.length == 2 && erroList.contains(password) && !erroList.contains(str[1])) {
					original.put("account", str[0]);
					original.put("password", str[1]);
					correct.putAll(changeKey(original));
					return;
				}
			}
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
		if (!erroList.contains(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = (String) original.get("cnote");
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

	public void cleanPhone(Map<String, Object> original, String field) {
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
		original.put("phone", buffer.toString());
		if (!erroList.contains(supersession)) {
			if (original.containsKey("cnote")) {
				String cnote = (String) original.get("cnote");
				original.put("cnote", cnote + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
	}

	public Map<String, Object> changeKey(Map<String, Object> map) {
		if (map.containsKey("phone")) {
			Object value = map.get("phone");
			map.remove("phone");
			map.put("mobilePhone", value);
		}
		if (map.containsKey("name")) {
			Object value = map.get("name");
			map.put("nameAlias", value);
		}
		//部分数据包含username
		if(map.containsKey("username")){
			Object value = map.get("username");
			map.put("userName", value);
			map.remove("username");
		}
		return map;
	}

}