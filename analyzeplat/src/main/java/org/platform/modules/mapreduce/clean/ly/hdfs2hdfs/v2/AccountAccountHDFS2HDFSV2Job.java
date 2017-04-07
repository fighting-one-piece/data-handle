package org.platform.modules.mapreduce.clean.ly.hdfs2hdfs.v2;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV2Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.DateFormatter;

public class AccountAccountHDFS2HDFSV2Job extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV2Mapper> getMapperClass() {
		return AccountHDFS2HDFSV2Mapper.class;
	}

	public static void main(String[] args) {
//		 args = new
//				 String[]{"hdfs://192.168.0.10:9000/warehouse_clean/account/20161219","hdfs://192.168.0.115:9000/ly"};
		if (args.length != 2) {
			System.out.println("参数不正确");
			System.exit(0);
		}
		String fss = args[0];
		String regex = "^(account)+.*$";
		int exitCode = 0 ;
		try {
			List<String> list = new ArrayList<String>();
			HDFSUtils.readAllFiles(fss, regex, list);
			String line;
			for(int i=0; i<list.size();i++){
				String path = list.get(i);
				System.out.println(">>>>>>>>执行文件<<<<<<<<<:  "+path);
				String fileName = path.substring(path.lastIndexOf("/"));
				if((line=DataCleanUtils.readAndDetectionFirstLine(path, "account"))!=null){
					String[] args1 = new String[] { line, path,args[1] +fileName};
					exitCode = ToolRunner.run(new AccountAccountHDFS2HDFSV2Job(), args1);
				}else{
					System.out.println("请检查文件："+path);
					System.exit(0);
				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class AccountHDFS2HDFSV2Mapper extends BaseHDFS2HDFSV2Mapper {

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
		if (!original.containsKey("insertTime")) {
			original.put("insertTime", DateFormatter.TIME.get().format(new Date()));
		}
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
				addCnote(original, idCard, "idCard");
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
				addCnote(original, email, "email");
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
				addCnote(original, phone, "phone");
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
		// 每条记录中，特有的字段
		int size = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			if (!erroList.contains((String) entry.getValue())) {
				times++;
			}
			if ("inputPerson".equals((String) entry.getKey()) || "sourceFile".equals((String) entry.getKey())
					|| "updateTime".equals((String) entry.getKey()) || "insertTime".equals((String) entry.getKey())) {
				size++;
			}
		}

		// 判断特殊字段名是否符合要求
		boolean flag2 = DataCleanUtils.judgeSpecialField(original);
		
			if (((sum > 0 || flag) && (times > size+1)) && flag2) {
				correct.putAll(changeKey(original));
			} else if(flag2){
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
			}else{
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
		String supersession = field.replaceAll(rex, ""); // 将不匹配的元素取出来
		if (supersession.length()>0 && !erroList.contains(supersession)) {
			//找到值后，不替换放入cnote字段
			addCnote(original, field, fieldName);
		}
		if(buffer.length()>0){
			original.put(fieldName, buffer.toString());
		}
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
		if(buffer.length()>0){
			original.put("phone", buffer.toString());
			if(supersession.length()>0 && !erroList.contains(supersession)){
				addCnote(original, field, "phone");
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
		// 部分数据包含username
		if (map.containsKey("username")) {
			Object value = map.get("username");
			map.put("userName", value);
			map.remove("username");
		}
		// 部分数据包含passWord
		if (map.containsKey("passWord")) {
			Object value = map.get("passWord");
			map.put("password", value);
			map.remove("passWord");
		}
		return map;
	}
	
	
	/**
	 * 根据传入的值，判断是否加入cnote
	 */
	public static void addCnote(Map<String, Object> original, String value, String fileName) {
		if (value != null && !erroList.contains(value)) {
			if (original.containsKey("cnote")) {
				String cnote = (String) original.get("cnote");
				original.put("cnote", cnote + "," + fileName + "__" + value);
			} else {
				original.put("cnote", fileName + "__" + value);
			}
		}
	}
}
