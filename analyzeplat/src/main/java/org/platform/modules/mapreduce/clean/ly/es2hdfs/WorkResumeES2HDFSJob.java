package org.platform.modules.mapreduce.clean.ly.es2hdfs;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class WorkResumeES2HDFSJob extends BaseES2HDFSJob {

	public static void main(String[] args) {
		/**
		 * 参数1：ES节点IP
		 * 参数2：ES节点Index/Type
		 * 参数3：HDFS输出路径
		 * 参数3：HDFS输出文件记录分割条数
		 */
		try {
			long starTime = System.currentTimeMillis();
			args = new String[]{"192.168.0.20", "work/resume",
					"hdfs://192.168.0.10:9000/warehouse_original/work/resume","80000"};
			int exitCode = ToolRunner.run(new WorkResumeES2HDFSJob(), args); 
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
