package org.platform.modules.mapreduce.clean.hzj.HDFS2HDFSJob.V2;

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV2Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class QQQQQunRelationHDFS2HDFSV2Job extends BaseHDFS2HDFSJob  {

	@Override
	public Class<? extends BaseHDFS2HDFSV2Mapper> getMapperClass() {
		// TODO Auto-generated method stub
		return QQQunRelationHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		 args = new String[] {
		 "hdfs://192.168.0.10:9000/warehouse_original/qq/qqqunrelation/20161220/",
		 "hdfs://192.168.0.10:9000/warehouse_clean/qq/qqqunrelation/20161220/"
		 };
		String fss = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem 	hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			int exitCode = 0;
			String line = null;
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS") && !p.getName().equals("part-r-00000")) {
					System.out.println("传入的路径:" + p);
					line = readFirstLine(hdfs, p);
					if (line != null) {
						System.out.println("----------------->>>>>metadata属性：" + line);
						String[] args1 = new String[] { line, p.toString(), args[1] + "/" + p.getName() };
						System.out.println("执行文件--------------->>>>>>>>>>>>>" + p.getName());
						exitCode = ToolRunner.run(new QQQQQunRelationHDFS2HDFSV2Job(), args1);
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
class QQQunRelationHDFS2HDFSMapper extends BaseHDFS2HDFSV2Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		original = CleanUtil.replaceSpace(original);
		int size = 0;
		boolean flag = false;
		/* 对QQ号进行验证 */
		if (original.containsKey("qqNum")) {
			String qqNum = (String) original.get("qqNum");
			if (CleanUtil.matchQQ(qqNum)) {
				cleanFiled(original, qqNum, CleanUtil.QQRex, "qqNum");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("sourceFile") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("qunNum")|| entry.getKey().equals("nick"))
						continue;
					if (CleanUtil.matchQQ((String) (entry.getValue()))) {
						String correctQQ = (String) entry.getValue();
						Object incorrectQQ = original.get("qqNum");
						entry.setValue(incorrectQQ);
						cleanFiled(original, correctQQ, CleanUtil.QQRex, "qqNum");
						cleanFlag = true;
						flag = true;
						size++;
						break;
					}
				}
				if (!cleanFlag) {	
					judge(qqNum, "qqNum", original);
				}

			}
		}
		/* 对QQ群号进行验证 */
		if (original.containsKey("qunNum")) {
			String qunNum = (String) original.get("qunNum");
			if (CleanUtil.matchNum(qunNum)) {
				cleanFiled(original, qunNum, CleanUtil.numRex, "qunNum");
				flag = true;
				size++;
			}else{
				judge(qunNum, "qunNum", original);
			}
		}
		
		 if (original.containsKey("nick")) {
				String nick = (String) original.get("nick");
				if(StringUtils.isNotBlank(nick) || !"NA".equals(nick)|| !"NULL".equals(nick)){
					size++;
				}
		 }
		
		
		
	
		
		int corriSize = 0;
		for (Entry<String, Object> entry : original.entrySet()) {
			String value = (String) entry.getValue();
			if (!"NA".equals((String) entry.getValue())&& StringUtils.isNotBlank(value)) {
				corriSize++;
			}
			if("updateTime".equals(entry.getKey())||"sourceFile".equals(entry.getKey())){
				size++;
			}
			
			
		}
		if (original.size()<=size && corriSize <= 3) {
			flag = false;
		}
        
		// 有正确的主要字段，且包含其它字段时为正确数据
		if (flag) {
			correct.putAll(original);
		} else {
			incorrect.putAll(map);
		}

	}

	/**
	 * 清洗
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


}