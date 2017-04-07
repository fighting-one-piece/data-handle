package org.platform.modules.mapreduce.clean.xx.util;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 * @author xiexin
 *
 */
public class Statistics {
	static String regex = "^(car)+.*$";
	private static void configureJob(Job job) {
		job.setJarByClass(Statistics.class);
		// 设置mapper类
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置reducer类
		job.setReducerClass(IntsumReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Gson gson;

		// 用来解决无法把json格式的数据转换为MAP
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				Set<Entry<String, Object>> entryset = original.entrySet();
				for (Entry<String, Object> entry : entryset) {
					if ("updateTime".equals(entry.getKey()) || "insertTime".equals(entry.getKey())
							|| "sourceFile".equals(entry.getKey()))
						continue;
					word = new Text(entry.getKey().toString());
					context.write(word, one);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class IntsumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		args = new String[] {
				"hdfs://192.168.0.10:9000//warehouse_data/financial/car/",
				"hdfs://192.168.0.115:9000/user/xx/wordcount/car/" };
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (inputArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "StatisticsCar");
		FileInputFormat.addInputPath(job, new Path(inputArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(inputArgs[1]));

		configureJob(job);
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}
