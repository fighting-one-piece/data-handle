package org.platform.modules.mapreduce.clean.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.platform.utils.DateFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCleanUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(DataCleanUtils.class);
	
	private static final String DELIMITER = "\\$#\\$";

	/**
	 * 判断特殊字段名是否符合要求
	 * 
	 * @param original
	 * @return true or false
	 */
	public static boolean judgeSpecialField(Map<String, Object> original) {
		if (!original.containsKey("inputPerson") || !original.containsKey("sourceFile")
				|| !original.containsKey("updateTime") || !original.containsKey("insertTime")) {
			return false;
		} else {
			String inputPerson = (String) original.get("inputPerson");
			String sourceFile = (String) original.get("sourceFile");
			String updateTime = (String) original.get("updateTime");
			String insertTime = (String) original.get("insertTime");
			if ("".equals(sourceFile) || "NA".equals(sourceFile) || "null".equals(sourceFile)
					|| "NULL".equals(sourceFile)) {
				return false;
			}
			if ("".equals(inputPerson) || "NA".equals(inputPerson) || "null".equals(inputPerson)
					|| "NULL".equals(inputPerson)) {
				return false;
			}
			// 严格按照此日期的格式匹配
			String dateRegx = "^\\d{4}-\\d{1,2}-\\d{1,2} \\d{2}:\\d{2}:\\d{2}$";
			if (updateTime == null || insertTime == null) {
				return false;
			}
			if (!updateTime.trim().matches(dateRegx) || !insertTime.trim().matches(dateRegx)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 读取并判断第一行
	 * 
	 * @param inputPath
	 * @param type
	 * @return
	 * @throws IOException
	 */
	public static String readAndDetectionFirstLine(String inputPath, String type) throws IOException {
		BufferedReader br = null;
		String line = null;
		Configuration conf = new Configuration();
		FileSystem filesystem = FileSystem.get(URI.create(inputPath), conf);
		FileStatus[] filestatus = filesystem.listStatus(new Path(inputPath));
		Path[] listPath = FileUtil.stat2Paths(filestatus);
		try {
			for (Path path : listPath) {
				FSDataInputStream fsDataInputStream = filesystem.open(path);
				br = new BufferedReader(new InputStreamReader(fsDataInputStream));
				if ((line = br.readLine()) != null) {
					String[] lineList = line.split(DELIMITER);
					List<String> indexList = Arrays.asList(indexReadSource("cleanIndex/allIndex.txt").split(DELIMITER));
					for (String string : lineList) {
						if (!indexList.contains(type + "_" + string) || line.endsWith("$#$")) {
							System.err.println("错误字段:"+string+"  ");
							return null;
						}
					}
				}
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		} finally {
			if (br != null) {
				br.close();
			}
		}
		return line;
	}

	/**
	 * 读取mapping文件
	 * 
	 * @param fileName
	 * @return
	 */
	public static String indexReadSource(String fileName) {
		InputStream in = null;
		String line = null;
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = DataCleanUtils.class.getClassLoader().getResourceAsStream("mapping/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			while (null != (line = br.readLine())) {
				sb.append(line).append("$#$");
			}
		} catch (Exception e) {
		} finally {
			try {
				if (null != br) {
					br.close();
				}
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
			}
		}
		return sb.delete(sb.lastIndexOf("$#$"), sb.length()).toString();
	}

	/**
	 * 把其他不匹配字段放入cnote
	 * 
	 * @param Filed
	 * @param FiledName
	 * @param original
	 * @return
	 */
	public static Map<String, Object> putCNote(String Filed, String FiledName, Map<String, Object> original) {
		if (!"NA".equals(Filed) && "NULL".equals(Filed) && "null".equals(Filed) && StringUtils.isNotBlank(Filed)) {
			if (original.containsKey("cnote")) {
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote + "," + FiledName + "__" + Filed);
			} else {
				original.put("cnote", FiledName + "__" + Filed);
			}

		}
		return original;

	}
	
	
	/**
	 * 检测导入HDFS的文件是否正确
	 * @param path		绝对路径
	 * @param type	需要检测的类型 例如：account  logistics
	 * @param regx	匹配需要检测文件的正则表达式
	 * @throws IOException 
	 */
	public static void detectionHDFSFile(String path, String type ,String regx) throws IOException{
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		BufferedReader br = null;
		try {
			hdfs = FileSystem.get(URI.create(path), conf);
			List<String> list = new ArrayList<String>();
			HDFSUtils.readAllFiles(path, regx, list);
			for (String str : list) {
				FileStatus fs = hdfs.getFileStatus(new Path(str));
				if (DataCleanUtils.readAndDetectionFirstLine(str, type) == null) {
					System.out.println("导入人:"+fs.getOwner()+",表头错误，请检查: " + str);
					continue;
				}
				String line = null;
				FSDataInputStream fin = hdfs.open(new Path(str));
				br = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				Map<String, Object> result = new HashMap<String, Object>();
				String[] lineListValue =null;
				if ((line = br.readLine()) != null) {
					String[] lineListKey = line.split("\\$#\\$");
					try {
						br.readLine();
						lineListValue = br.readLine().split("\\$#\\$");
					} catch (Exception e) {
						System.out.println("导入人:"+fs.getOwner()+",行数太少，请确认："+str);
						continue;
					}
					for (int i = 0, len = lineListKey.length; i < len; i++) {
						try {
							result.put(lineListKey[i], lineListValue[i]);
						} catch (Exception e) {
							System.out.println("导入人:"+fs.getOwner()+",表头和数据不能对应：");
						}
					}
				}
				if (!result.containsKey("insertTime")) {
					result.put("insertTime", DateFormatter.TIME.get().format(new Date()));
				}
				if(!DataCleanUtils.judgeSpecialField(result)){
					System.out.println("导入的记录出错： "+str);
					System.out.println(result);
				}
			}
			System.out.println("检测完毕！");
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(br!=null){
				br.close();
			}
			if(hdfs!=null){
				hdfs.close();
			}
		}
	}
	
	/**
	 * 随机读取HDFS上的文件
	 * @param path 路径
	 * @param size		生成随机数范围（数值越大，读的条数越少,最好与文件的总行数接近）
	 * @throws IOException
	 */
	public static void randomReadHDFSFile(String path ,int size) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs;
		BufferedReader br=null;
		FSDataInputStream  in = null;
		try {
			fs = FileSystem.get(URI.create(path),conf);
			in =fs.open(new Path(path));
			long fileLength = fs.getFileStatus(new Path(path)).getLen();
			Random random = new Random();
			br = new BufferedReader(new InputStreamReader(in));
			for (long currentPosition = 0L; currentPosition < fileLength; ) {
				in.seek(currentPosition);
				System.out.println(br.readLine());
				currentPosition +=  random.nextInt(size);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if(br!=null){
				br.close();
			}
			if(in!=null){
				in.close();
			}
		}
	}
	
	
	
/**
 * 清洗字段
 * @param original		集合
 * @param fileName		字段名
 * @param regex		正则
 *  @param isFlag		清洗最后一个字段时判断
 * @param filterField 	过滤字段
 */
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
				String key = entry.getKey();
				if (key.equals("_id") || key.equals("sourceFile") || key.equals("insertTime")|| key.equals("updateTime") || fileList.contains(key))continue;	
				String value = entry.getValue().toString();
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(value);
				if (matcher.find()) { // 判断是否匹配
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
					entry.setValue(filter);
					cleanCNote(original,filter,key,value);
					break;
				}
			}
		}
		
		if (!flag) {
			if(original.containsKey(fileName)){
				String value = original.get(fileName).toString();
				cleanCNote(original,value,fileName,value);
				original.remove(fileName);
			}
		}
		
		if("yes".equals(isFlag)){
			original.remove("temporary");
		}
	}
	/**
	 * 处理cnote字段
	 * @param original
	 * @param filter
	 * @param fileName
	 * @param fileValue
	 */
	private static void cleanCNote(Map<String, Object> original,String filter,String fileName,String fileValue){
		if (StringUtils.isNotBlank(filter) && !"NA".equals(filter)) {
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
