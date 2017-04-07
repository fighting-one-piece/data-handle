package org.platform.modules.mapreduce.clean.skm.ES2HDFS;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;
/**
 * 将数据导入Hadoop
 * @author Administrator
 *
 */
public class WorkCybercafeES2HDFSJob extends BaseES2HDFSJob {

	public static void main(String[] args) {
		/**
		 * 参数1：ES节点IP
		 * 参数2：ES节点Index/Type
		 * 参数3：HDFS输出路径
		 * 参数3：HDFS输出文件记录分割条数
		 */
		try {
			args = new String[]{"192.168.0.10","work/cybercafe","hdfs://192.168.0.10:9000/elasticsearch_original/work/cybercafe/10","1000000"};
			int exitCode = ToolRunner.run(new WorkCybercafeES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
