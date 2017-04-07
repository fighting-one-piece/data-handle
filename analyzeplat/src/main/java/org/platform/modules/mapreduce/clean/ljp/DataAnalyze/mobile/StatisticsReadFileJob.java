package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.mobile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.Set;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.platform.modules.elasticsearch.clean.util.CleanUtil;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class StatisticsReadFileJob {
	private static final String DELIMITER = "\\$#\\$";
	private static List<String> list = new ArrayList<String>();
	
	static{
		list.add("originalLinkAddress");
		list.add("originalLinkRegion");
		list.add("destination");
		list.add("objectiveRegion");
		list.add("city");
		list.add("linkCity");
		list.add("county");
		list.add("linkCounty");
	}
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		InputStream in = null;
		BufferedReader br = null;
		OutputStream out = null;
		BufferedWriter bw = null;
		try {
			in = new FileInputStream(new File("D:\\DataStatisticsAndAnalysis\\Statistic\\fruit.txt"));
			br = new BufferedReader(new InputStreamReader(in));
			out = new FileOutputStream(new File("D:\\DataStatisticsAndAnalysis\\Statistic\\fruit2.txt"));
			bw = new BufferedWriter(new OutputStreamWriter(out));
			String line = null;
			 List<String> lists = Arrays.asList(DataCleanUtils.indexReadSource("ljp/addressCode").split(DELIMITER));
			Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
			while ((line = br.readLine()) != null) {
				Map<String, Object> original = gson.fromJson(line.toString(), Map.class);
					for(String listName : list){
						if(original.containsKey(listName)){
							String string = removeEnglish((String)original.get(listName)).trim();
							for(int i=0;i<lists.size();i++){
								String indexValue = lists.get(i);
								if(indexValue.contains(string)){
									original.put(listName, indexValue.substring(indexValue.indexOf("_")+1));
								}
							}
						}
					}
					bw.write(gson.toJson(original));
					bw.newLine();
				}
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in)
					in.close();
				if (null != br)
					br.close();
				if (null != out)
					out.close();
				if (null != bw)
					bw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/***
	 * 去除值里面的英文
	 * 
	 * @param date
	 * @return
	 */
	public static String removeEnglish(String date) {
		Pattern p = Pattern.compile(CleanUtil.englishRex);
		Matcher m = p.matcher(date);
		return m.replaceAll("");
	}
	
}	
	

// Map<String, Object> original = gson.fromJson(line, Map.class);
// if (original.containsKey("linkName") && (CleanUtil.matchChinese((String)
// original.get("linkName"))
// || CleanUtil.matchEnglish((String) original.get("linkName")))) {
// list.add((String) original.get("linkName"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("linkPhone") && (CleanUtil.matchPhone((String)
// original.get("linkPhone"))
// || CleanUtil.matchCall((String) original.get("linkPhone")))) {
// list.add((String) original.get("linkPhone"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("linkAddress") && !CleanUtil.matchNum((String)
// original.get("linkAddress"))) {
// list.add((String) original.get("linkAddress"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("sendCompany") && !CleanUtil.matchNum((String)
// original.get("sendCompany"))) {
// list.add((String) original.get("sendCompany"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("name") && (CleanUtil.matchChinese((String)
// original.get("name"))
// || CleanUtil.matchEnglish((String) original.get("name")))) {
// list.add((String) original.get("name"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("idCode") && !"".equals((String)
// original.get("idCode"))
// && !"NA".equals((String) original.get("idCode"))) {
// list.add((String) original.get("idCode"));
// } else {
// list.add((String) original.get("phone"));
// }
//
// if (original.containsKey("address") && !CleanUtil.matchNum((String)
// original.get("address"))) {
// list.add((String) original.get("address"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("expressId") && CleanUtil.matchNum((String)
// original.get("expressId"))) {
// list.add((String) original.get("expressId"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("good") && !CleanUtil.matchNum((String)
// original.get("good"))) {
// list.add((String) original.get("good"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("goodNumber") && CleanUtil.matchNum((String)
// original.get("goodNumber"))) {
// list.add((String) original.get("goodNumber"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("receiver") && CleanUtil.matchNum((String)
// original.get("receiver"))) {
// list.add((String) original.get("receiver"));
// } else {
// list.add("NA");
// }
//
// if (original.containsKey("begainTime")) {
// list.add((String) original.get("begainTime"));
// } else {
// list.add("NA");
// }
// for (int i = 0; i < list.size(); i++) {
// hk.append(list.get(i)).append("\t");
// }
// hk.deleteCharAt(hk.length() - 1);
// bw.write(hk.toString());
// bw.newLine();
// hk = new StringBuilder();
// list = new ArrayList<String>();
