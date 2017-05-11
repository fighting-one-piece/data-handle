package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** HDFS数据文件批量导入ES5 */
public abstract class BaseHDFS2ES5V1Job extends BaseJob {
	
	public abstract Class<? extends BaseHDFS2ES5V1Mapper> getMapperClass();
	
	/**
	 * 参数1：ES INDEX
	 * 参数2：ES TYPE
	 * 参数3：HDFS输入路径
	 * 参数4：HDFS输出路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 4) {
			LOG.error("error! need four parameters at least!");
			System.exit(2);
		}
		conf.set("esIndex", oArgs[0]);
		conf.set("esType", oArgs[1]); 
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(getClass());
		job.setMapperClass(getMapperClass());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(BaseHDFS2ES5V1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 2; i < oArgs.length - 1; i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
		FileOutputFormat.setOutputPath(job, new Path(oArgs[oArgs.length - 1]));
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
