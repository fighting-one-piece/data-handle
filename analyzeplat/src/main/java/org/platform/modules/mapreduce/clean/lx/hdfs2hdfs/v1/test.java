package org.platform.modules.mapreduce.clean.lx.hdfs2hdfs.v1;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;


public class test {
	public static void fieldValidate(Map<String, Object> original, String fileName, String regex,String isFlag ,String... filterField) {
		List<String> fileList = Arrays.asList(filterField);
		StringBuilder sBuilder = new StringBuilder();
		boolean flag = false;
		if (original.containsKey(fileName)) {
			String value = original.get(fileName).toString();
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(value);
			if(matcher.find()){		
				Matcher matchers = pattern.matcher(value);
						flag = true;
						while (matchers.find()) {
							sBuilder.append(matcher.group() + ",");
						}
						if (sBuilder.length() > 0) {
							sBuilder.deleteCharAt(sBuilder.length() - 1);
						}
						String filter = value.replace(sBuilder.toString(), "");
						cleanCNote(original,filter,fileName,value);
						original.put(fileName, sBuilder.toString());
			}else {
				for (Map.Entry<String, Object> entry : original.entrySet()) {
					String key = entry.getKey();
					String values = (String) entry.getValue();
					Matcher matchers = pattern.matcher(values);					
					if (key.equals("_id") || key.equals("sourceFile") || key.equals("insertTime")|| key.equals("updateTime") || fileList.contains(key))continue;
					if (matchers.find()) { 
						flag = true;
						entry.setValue((String) original.get(fileName));
						String corr = values;
						Matcher m = pattern.matcher(corr);
						while (m.find()) {
							sBuilder.append(m.group() + ",");
						}
						if (sBuilder.length() > 0) {
							sBuilder.deleteCharAt(sBuilder.length() - 1);
						}
						String filter = corr.replace(sBuilder.toString(), "");
						cleanCNote(original,filter,key,values);
						original.put(fileName, sBuilder.toString());
						break;
					}
				}
			}
		}else {
			for (Map.Entry<String, Object> entry : original.entrySet()) {	
				System.out.println("a");
				String key = entry.getKey();
				if (key.equals("_id") || key.equals("sourceFile") || key.equals("insertTime")|| key.equals("updateTime") || fileList.contains(key))continue;	
				String value = entry.getValue().toString();
				Pattern pattern = Pattern.compile(regex);
				System.out.println(value);
				Matcher matcher = pattern.matcher(value);
				if (matcher.find()) { // 判断是否匹配
					System.out.println("进图");
					Matcher matchers = pattern.matcher(value);
					flag = true;
					while (matchers.find()) {
						sBuilder.append(matcher.group() + ",");
					}
					if (sBuilder.length() > 0) {
						sBuilder.deleteCharAt(sBuilder.length() - 1);
					}
					original.put(fileName, sBuilder.toString());
					String filter = value.replace(sBuilder.toString(), "");
					cleanCNote(original,filter,key,value);
					break;
				}
			}
		}
		if (!flag) {
			String value = original.get(fileName).toString();
			cleanCNote(original,value,fileName,value);
			original.remove(fileName);
		}
		if("yes".equals(isFlag)){
			original.remove("temporary");
		}
	}
	
	public static void cleanCNote(Map<String, Object> original,String filter,String fileName,String fileValue){
		if (StringUtils.isNotBlank(filter) && !"NA".equals(filter)&&!"cnote".equals(fileName)) {
			if (original.containsKey("cnote")) {	
					String note = original.get("cnote").toString();
					if (StringUtils.isNotBlank(note) && !"NA".equals(note)) {
						original.put("cnote", note + "," + fileName + "_" + fileValue);
						original.put("temporary", note + "," + filter);
					} else {
						original.put("cnote", fileName + "_" + fileValue);
						original.put("temporary", filter);
					}
				} else {
					original.put("cnote", fileName + "_" + fileValue);
					original.put("temporary", filter);
				}
			}
		}
	
}
