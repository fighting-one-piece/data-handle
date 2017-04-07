package org.platform.modules.mapreduce.clean.skm.HDFS2HDFS.V1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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
 * 网吧数据清洗
 * @author tjl
 *
 */
public class WorkCybercafeHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return CybercafeHDFS2HDFSMapper.class;
	}
	
	public static void main(String[] args) {
		
		try {
			long starTime = System.currentTimeMillis();
			String n;
			int exitCode = 0;
			for (int j = 1; j <= 1; j++) {
				for (int i = 0; i <= 9; i++) {
					if (i < 10) {
						n = "0" + i;
					} else {
						n = "" + i;
					}
					// args = new
					// String[]{"hdfs://192.168.0.115:9000/elasticsearch/tjl/ot/records-"+index+"-m-000"+n,
					// "hdfs://192.168.0.115:9000/elasticsearch/tjl/ot_out/records-"+index+"-m-000"+n};
					args = new String[] {"",
							"hdfs://192.168.0.10:9000/elasticsearch_original/work/cybercafe/105/records-" + j + "-m-000" + n,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/cybercafe/105/records-" + j + "-m-000"
									+ n };
					System.out.println(args[0] + ":" + args[1]);
					exitCode = ToolRunner.run(new WorkCybercafeHDFS2HDFSJob(), args);
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

class CybercafeHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper{

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		original = replaceSpace(original);
		int size = 5;
		boolean flag = false;
		//清洗idCard字段
		//判断是否包含idCard字段
		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			//判断字段中是否包含正确的身份证号
			if (CleanUtil.matchIdCard(idCard)){
				//包含则将包含的身份证号提取出来放入idCard，提取后剩下的数据append到cnote上
				cleanField(original, idCard, CleanUtil.idCardRex, "idCard");
				flag = true;
			} else {//如果不包含身份证号则可能错列
				boolean cleanFlag = false;
				//遍历map，跳过特殊字段，判断是否有包含idCard的字段
				for (Entry<String, Object> entry : original.entrySet()){
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
						continue;
					//如果有包含idCard的字段，则将其与idCard字段值进行交换
					if (CleanUtil.matchIdCard((String)(entry.getValue()))){
						String correctIdCard = (String)entry.getValue();
						Object incorrectIdCard = original.get("idCard");
						entry.setValue(incorrectIdCard);
						//身份证号提取出来放入idCard，提取后剩下的数据append到address上
						cleanField(original, correctIdCard, CleanUtil.idCardRex, "idCard");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				//如果没有找到身份证号，则idcard字段为废值，将其值append到address上，并将idCard字段设为"NA"
				if (!cleanFlag) {
					size++;
					judge(idCard, "idCard", original);
				}
			}
		}	
		if (original.containsKey("cybercafeIp")) {
			String IP = (String) original.get("cybercafeIp");
			if (CleanUtil.matchIP(IP)) {
				cleanField(original, IP, CleanUtil.IPRex, "cybercafeIp");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
						continue;
					if (CleanUtil.matchIP((String)(entry.getValue()))) {
						String correctIP = (String)entry.getValue();
						Object incorrectIP = original.get("cybercafeIp");
						entry.setValue(incorrectIP);
						cleanField(original, correctIP, CleanUtil.IPRex, "cybercafeIp");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					judge(IP, "cybercafeIp", original);
				}
			}
		}	
		if (original.containsKey("qq")) {
			String qq = (String) original.get("qq");
			if (CleanUtil.matchQQ(qq)) {
				cleanField(original, qq, CleanUtil.QQRex, "qq");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
						|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
						|| entry.getKey().equals("idCard"))
						continue;
					if (CleanUtil.matchQQ((String)(entry.getValue()))) {
						String correctQQ = (String)entry.getValue();
						Object incorrectQQ = original.get("qq");
						entry.setValue(incorrectQQ);
						cleanField(original, correctQQ, CleanUtil.QQRex, "qq");
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
		
		//有正确的主要字段，且包含其它字段时为正确数据
		if (flag){
			correct.putAll(changeKey(original));
		} else {
			incorrect.putAll(map);
		}
		
	}
	
	/**
	 * 将废值替换为NA
	 * @param field 字段
	 * @param fieldName 字段名
	 * @param original  总值Map
	 */
	public void judge(String field, String fieldName, Map<String, Object> original) {
		if (!"NA".equals(field) && StringUtils.isNotBlank(field)) {
			if (original.containsKey("cnote")) {
				//如果address中的值为"NA"，则要将其换位空字符串
				String address = ((String) original.get("cnote")).equals("NA") ? "":(String) original.get("cnote");
				original.put("cnote", address + field);
			} else {
				original.put("cnote", field);
			}
		}
		original.put(fieldName, "NA");
	}
	
	/**
	 * 
	 * @param original    存储数据的Map
	 * @param field		      字段值
	 * @param rex		      匹配的正则表达式
	 * @param fieldName   字段名
	 */
	public void cleanField (Map<String,Object> original , String field, String rex, String fieldName){
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
		if (StringUtils.isNotBlank(supersession)) {
			if (original.containsKey("cnote")) {
				String address = ((String) original.get("cnote")).equals("NA") ? "":(String) original.get("cnote");
				original.put("cnote",address + supersession);
			} else {
				original.put("cnote", supersession);
			}
		}
		original.put(fieldName, toBuffer);
	}
	
	
	public Map<String, Object> changeKey(Map<String, Object> map){
		if (map.containsKey("qq")){
			Object value = map.get("qq");
			map.remove("qq");
			map.put("qqNum", value);
		}
		if (map.containsKey("name")) {
			Object value = map.get("name");
			map.put("nameAlias", value);
		}
		if (map.containsKey("cybercafeName")){
			Object value = map.get("cybercafeName");
			map.remove("cybercafeName");
			map.put("cybercafe", value);
		}
		return map;
		
	}
	
	public Map<String,Object> replaceSpace(Map<String,Object> map){
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String,Object> entry = it.next();
			if ("insertTime".equals(entry.getKey()) || "updateTime".equals(entry.getKey())
					|| "onlineTime".equals(entry.getKey()))continue;
			entry.setValue((String.valueOf(entry.getValue())).replaceAll("\\s", "").replaceAll("", ""));
		}
		return map;
	}

	
}