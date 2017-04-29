package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** HDFS数据文件批量导入Titan */
public abstract class BaseHDFS2TitanV1Job extends BaseJob {
	
	public abstract Class<? extends BaseHDFS2TitanV1Mapper> getMapperClass();
	
	/**
	 * 参数1：TOPIC主题
	 * 参数2：HDFS输入路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		conf.set("topic", args[0]);
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 2) {
			LOG.error("error! need two parameters at least!");
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
		for (int i = 1; i < oArgs.length; i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
