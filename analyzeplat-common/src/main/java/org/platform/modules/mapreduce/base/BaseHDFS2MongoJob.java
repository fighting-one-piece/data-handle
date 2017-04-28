package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public abstract class BaseHDFS2MongoJob extends BaseJob {
	
	/**
	 * 获取Mapper类
	 * @return
	 */
	public abstract Class<? extends Mapper<LongWritable, Text, NullWritable, Text>> getMapperClass();
	
	/**
	 * 参数1：Mongo 数据库名称
	 * 参数2：Mongo 数据表名称
	 * 参数3：批量大小
	 * 参数5：Map分割大小，单位M
	 * 参数4：HDFS输入路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("databaseName", args[0]);
		conf.set("collectionName", args[1]); 
		conf.set("batchSize", args[2]); 
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", Long.parseLong(args[3]) * 1024 * 1024);
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 5) {
			LOG.error("error! need 5 input parameters!");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(BaseHDFS2MongoJob.class);
		job.setMapperClass(getMapperClass());
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(NullOutputFormat.class);
		
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 4; i < args_len; i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
