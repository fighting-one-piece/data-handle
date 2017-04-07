package org.platform.modules.mapreduce.clean.hzj.Statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class StatisticalData {

	static FileSystem fs = null;
	static Configuration conf = new Configuration();
	static String regex = "^(part)+.*$";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		String paths = "hdfs://192.168.0.10:9000/warehouse_data/work/hospital_new/";
		String pathOut = "hdfs://192.168.0.115:9000/elasticsearch/work/hospital_new/";
		List<String> files = new ArrayList<>();
		HDFSUtils.readAllFiles(paths, regex, files);
		files.add(pathOut);
		args = files.toArray(new String[0]);
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 创建job对象
		Job job = Job.getInstance(conf, "StatisticaHospitalData");
		// 设置运行的类
		job.setJarByClass(StatisticalData.class);
		// 设置mapper类
		job.setMapperClass(StatisticsFileMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置reducer类
		job.setReducerClass(StatisticsFileReduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int args_len = inputArgs.length;

		StringBuilder inputPaths = new StringBuilder();
		for (int j = 0; j < (args_len - 1); j++) {
			inputPaths.append(inputArgs[j]).append(",");
		}
		if (inputPaths.length() > 0)
			inputPaths.deleteCharAt(inputPaths.length() - 1);

		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(inputArgs[args_len - 1]));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}

	static class StatisticsFileMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		// 设置固定值为1
		private IntWritable one = new IntWritable(1);

		// private static Text line = new Text();
		private Gson gson;

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			@SuppressWarnings("unchecked")
			Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
			try {
				// System.out.println(original .toString());
				Set<Entry<String, Object>> entries = original.entrySet();
				for (Entry<String, Object> entry : entries) {
					if ("updateTime".equals(entry.getKey()) || "insertTime".equals(entry.getKey())
							|| "sourceFile".equals(entry.getKey()))
						continue;
					context.write(new Text(entry.getKey().toString()), one);
				}
			} catch (Exception e) {
				System.out.println("异常" + original);
			}

		}
	}

	public static class StatisticsFileReduce extends Reducer<Text, Text, Text, LongWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : value) {
				sum += Long.parseLong(val.toString());
			}
			/*
			 * Iterator <IntWritable> iterable = values.iterator();
			 * while(iterable.hasNext()){ sum+=1; }
			 */
			context.write(key, new LongWritable(sum));
		}
	}
}
