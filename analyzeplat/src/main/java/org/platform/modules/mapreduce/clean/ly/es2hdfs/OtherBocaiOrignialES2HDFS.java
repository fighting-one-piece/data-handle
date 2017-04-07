package org.platform.modules.mapreduce.clean.ly.es2hdfs;


import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class OtherBocaiOrignialES2HDFS extends BaseES2HDFSJob{
	public static void main(String[] args) {
		/**
		 * 参数1：ES节点IP
		 * 参数2：ES节点Index/Type
		 * 参数3：HDFS输出路径
		 * 参数3：HDFS输出文件记录分割条数
		 */
		try {
			args = new String[]{"192.168.0.20", "other_new/bocaioriginal",
					"hdfs://192.168.0.10:9000/warehouse_original/other/bocaioriginal","1200000"};
			int exitCode = ToolRunner.run(new OtherBocaiOrignialES2HDFS(), args); 
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
