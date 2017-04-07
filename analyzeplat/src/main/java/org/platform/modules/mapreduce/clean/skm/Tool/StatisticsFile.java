package org.platform.modules.mapreduce.clean.skm.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
import org.platform.utils.JobUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * 
 *统计字段比重
 */
public class StatisticsFile {

	static String paths = "hdfs://192.168.0.10:9000/warehouse_data/work/cybercafe";
	//	static String paths = "hdfs://192.168.0.115:9000/skm/business";
	static String pathOut = "hdfs://192.168.0.115:9000/skm1/cybercafe";
	static String regex = "^(cybercafe)+.*$";
	static FileSystem fs = null;
	static Configuration conf = new Configuration();	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		List<String> files = new ArrayList<>();
		HDFSUtils.readAllFiles(paths, regex, files);	
		files.add(pathOut);
		args = files.toArray(new String[0]);
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 创建job对象
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName()); 
		Job job = Job.getInstance(conf, "CabercafeAcount");
		// 设置运行的类
		job.setJarByClass(StatisticsFile.class);
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
	
		private Gson gson =null;

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				@SuppressWarnings("unchecked")			
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				Set<Entry<String, Object>> entries = original.entrySet();

				for (Entry<String, Object> entry : entries) {
	
					context.write(new Text(entry.getKey().toString()),new IntWritable(1));
				}
				
				//System.out.println(original.toString());

			} catch (Exception e) {
			}	
		}
	}
	static class StatisticsFileReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}	

			context.write(key, new IntWritable(sum));
		}
	}
}
