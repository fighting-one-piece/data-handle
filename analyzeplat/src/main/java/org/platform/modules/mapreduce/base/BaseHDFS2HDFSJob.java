package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public abstract class BaseHDFS2HDFSJob extends BaseJob {
	
	/**
	 * 获取Mapper类
	 * @return
	 */
	public abstract Class<? extends BaseHDFS2HDFSMapper> getMapperClass();
	
	/**
	 * 参数1：HDFS输入路径
	 * 参数2：HDFS输出路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		//主要针对文本格式，JSON格式的可以随意设置这个属性
		conf.set("column.metadata", args[0]);
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 3) {
			LOG.error("args error! need column metadata and input path and output path");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(getClass());
		job.setMapperClass(getMapperClass());
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 1; i < (args_len - 1); i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(oArgs[args_len - 1]));
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}
	
}
