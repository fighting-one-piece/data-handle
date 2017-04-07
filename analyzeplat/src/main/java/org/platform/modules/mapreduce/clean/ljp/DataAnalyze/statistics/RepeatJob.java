package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 根据某个条件进行数据去重
 * @author Administrator
 *
 */
public class RepeatJob extends Configured implements Tool {

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "logisticsStatistics");
		job.setJarByClass(RepeatJob.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 0; i < (args_len - 1); i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0)
			inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(oArgs[args_len - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			long timeStar = System.currentTimeMillis();
			args = new String[] { "hdfs://192.168.0.115:9000/user/ljpTest/input/CombineTest",
					"hdfs://192.168.0.115:9000/user/ljpTest/output2" };
			int ret = ToolRunner.run(new RepeatJob(), args);
			long timeEnd = System.currentTimeMillis();

			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text(""), value);
		}
	}

	public static class CountReducer extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for (Text T : value) {
				context.write(NullWritable.get(), T);
			}
		}
	}
}