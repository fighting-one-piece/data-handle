package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** HDFS数据文件批量更新ES数据 */
public class BaseHDFS2ESV3Job extends BaseJob {
	
	/**
	 * 参数1：ES Index
	 * 参数2：ES Type
	 * 参数3：ES 集群名称
	 * 参数4：ES 集群IP
	 * 参数5：ES 批量导入大小
	 * 参数6：HDFS输入路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("esIndex", args[0]);
		conf.set("esType", args[1]); 
		conf.set("esClusterName", args[2]); 
		conf.set("esClusterIP", args[3]); 
		conf.set("batchSize", args[4]); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 6) {
			LOG.error("error! need 6 input parameters!");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(BaseHDFS2ESV3Job.class);
		job.setMapperClass(BaseHDFS2ESV3Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(NullOutputFormat.class);
		
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 5; i < args_len; i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
