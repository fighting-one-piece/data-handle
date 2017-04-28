package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** HDFS数据文件批量导入ES5 */
public abstract class BaseHDFS2ES5V1Job extends BaseJob {
	
	public abstract Class<? extends BaseHDFS2ES5V1Mapper> getMapperClass();
	
	/**
	 * 参数1：ES INDEX
	 * 参数2：ES TYPE
	 * 参数3：HDFS输入路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		conf.set("esIndex", args[0]);
		conf.set("esType", args[1]); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 3) {
			LOG.error("error! need three parameters at least!");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(getClass());
		job.setMapperClass(getMapperClass());
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 2; i < oArgs.length; i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
