package org.platform.modules.mapreduce.clean.skm.HDFS2ES.V1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

/**
 * 清洗后数据导入ES
 * 
 * @author Administrator
 *
 */
public class WorkCybercafeHDFS2ESJob extends BaseHDFS2ESV1Job {
	public static void main(String[] args) {
		try {	
			List<String> filePathList = new ArrayList<String>();
			String regex = "^(correct)+.*$";
			HDFSUtils.readAllFiles(args[0],regex, filePathList);	
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = filePathList.size(); i < len; i++) {
				sb.append(filePathList.get(i)).append(",");
				if ((i % 20 == 0 || i == (len - 1))&&i!=0) {
					sb.deleteCharAt(sb.length() - 1);
					String[]  args1= new String[] { "work", "cybercafe", "cisiondata", 
							"192.168.0.10,192.168.0.12", sb.toString(),args[1]+i};
					ToolRunner.run(new WorkCybercafeHDFS2ESJob(),args1);
					sb = new StringBuilder();
					
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
