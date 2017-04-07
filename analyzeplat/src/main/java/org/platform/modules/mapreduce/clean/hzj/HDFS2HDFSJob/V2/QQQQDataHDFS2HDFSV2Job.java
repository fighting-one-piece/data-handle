package org.platform.modules.mapreduce.clean.hzj.HDFS2HDFSJob.V2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV2Mapper;
import org.platform.modules.mapreduce.clean.ljp.HDFS2HDFS.V2.FinancialLogisticsHDFS2HDFSV2Job;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;

public class QQQQDataHDFS2HDFSV2Job extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV2Mapper> getMapperClass() {
		return QQHDFS2HDFSMapper.class;
	} 
		 public static void main(String[] args) {
				long startTime = System.currentTimeMillis();
				int exitCode = 0;
				try{
					args = new String[]{ "hdfs://192.168.0.115:9000/warehouse_data/qq/qqdata/qqdata__04a7e600_2733_48c6_9bf2_33f6a41939d6",
							 "hdfs://192.168.0.115:9000/warehouse_clean/qq/qqdata/20170316"};
					String line = DataCleanUtils.readAndDetectionFirstLine(args[0],"qqdata");
					if(line != null){
					String[] str = new String[] {line,args[0],args[1]};
					ToolRunner.run(new FinancialLogisticsHDFS2HDFSV2Job(), str);
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

class QQHDFS2HDFSMapper extends BaseHDFS2HDFSV2Mapper {
	static List<String> list = new ArrayList<String>();
	static List<String> erroList = new ArrayList<String>();
	static Object[] string = list.toArray();
	static {
		erroList.add("NA");
		erroList.add("NULL");
		erroList.add("");
		erroList.add("null");
	}
	static {
		list.add("_id");
		list.add("insertTime");
		list.add("sourceFile");
		list.add("updateTime");
		list.add("emailpwd");
		list.add("qqPassword");
		list.add("security");
		list.add("qqPoint");
		list.add("cnote");
	}
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.putAll(original);
		original = CleanUtil.replaceSpace(original);
		int size = 1;
		boolean flag = false;
		/* 对QQ号进行验证 */
		/*if (original.containsKey("qq")) {
			String qq = (String) original.get("qq");
			if (CleanUtil.matchQQ(qq)&&!qq.contains(",")) {
				cleanFiled(original, qq, CleanUtil.QQRex, "qq");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if ( entry.getKey().equals("sourceFile") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("email")|| entry.getKey().equals("qqPassword")|| entry.getKey().equals("emailpwd")
							|| entry.getKey().equals("security")|| entry.getKey().equals("qqPoint"))
						continue;
					if (CleanUtil.matchQQ((String) (entry.getValue()))&&!qq.contains(",")) {
						String correctQQ = (String) entry.getValue();
						Object incorrectQQ = original.get("qq");
						entry.setValue(incorrectQQ);
						cleanFiled(original, correctQQ, CleanUtil.QQRex, "qq");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(qq, "qq", original);
				}

			}
		}*/
		
		/**  对QQ号进行验证 */
		if (original.containsKey("qq")) {
			DataCleanUtils.fieldValidate(original, "qq", CleanUtil.QQRex,string.toString());
			flag = true;
		    size++;
		}
		if (original.containsKey("ip")) {
			DataCleanUtils.fieldValidate(original, "ip", CleanUtil.IPRex,string.toString());
			flag = true;
		    size++;
		}
		if (original.containsKey("email")) {
			DataCleanUtils.fieldValidate(original, "email", CleanUtil.emailRex,string.toString());
			flag = true;
		    size++;
		}
		if(original.containsKey("netDate")){
			original.put("netDate", DateValidJob.FormatDate(original.get("netDate").toString()));
		}
		
		
		
		/* 对ip进行验证 */
		if (original.containsKey("ip")) {
			String ip = (String) original.get("ip");
			if (CleanUtil.matchIP(ip)) {
				cleanFiled(original, ip, CleanUtil.IPRex, "ip");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("sourceFile")|| entry.getKey().equals("updateTime")
							|| entry.getKey().equals("emailpwd")|| entry.getKey().equals("security")
							|| entry.getKey().equals("qqPassword"))
						continue;
					if (CleanUtil.matchIP((String) (entry.getValue()))) {
						String correctIP = (String) entry.getValue();
						Object incorrectIP = original.get("ip");
						entry.setValue(incorrectIP);
						cleanFiled(original, correctIP, CleanUtil.IPRex, "ip");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(ip, "ip", original);
				}

			}
		}
		/* 邮箱验证 */
		if (original.containsKey("email")) {
			String email = ((String) original.get("email"));
			if (CleanUtil.matchEmail(email)) {
				cleanFiled(original, email, CleanUtil.emailRex, "email");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if ( entry.getKey().equals("sourceFile")|| entry.getKey().equals("updateTime")
							|| entry.getKey().equals("qq")|| entry.getKey().equals("qqPassword")
							|| entry.getKey().equals("emailpwd")|| entry.getKey().equals("security"))
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

		
		int corriSize = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			String value = (String) entry.getValue();
			String qqPassword = (String) original.get("qqPassword");
			
			if (!"NA".equals((String) entry.getValue()) && StringUtils.isNotBlank(value)
					){
				corriSize++;
			}
			if("updateTime".equals(entry.getKey())||"sourceFile".equals(entry.getKey())
					&& StringUtils.isNotBlank(qqPassword)){
				size++;
			}
		}	
		if (original.size()<=size && corriSize <= 3) {
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
	 * 清洗QQ号
	 */
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
		if (StringUtils.isNotBlank(supersession)&&!"NA".equals(supersession)&&!"NULL".equals(supersession)) {
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
	 * 将废值替换成NA
	 */
	public void judge(String field, String fieldName, Map<String, Object> original) {
		if (!"NA".equals(field) && StringUtils.isNotBlank(field)&&!"NULL".equals(field)) {
			if (original.containsKey("cnote")) {
				// 如果cnote中的值为"NA"，则要将其换位空字符串
				String address = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", address + field);
			} else {
				original.put("cnote", field);
			}
		}
		original.put(fieldName, "NA");
	}

	public Map<String, Object> changeKey(Map<String, Object> map) {
		if (map.containsKey("qq")) {
			Object value = map.get("qq");
			map.remove("qq");
			map.put("qqNum", value);
		}
		if (map.containsKey("emailpwd")) {
			Object value = map.get("emailpwd");
			map.remove("emailpwd");
			map.put("emailPassword", value);
		}

		return map;
	}

}
