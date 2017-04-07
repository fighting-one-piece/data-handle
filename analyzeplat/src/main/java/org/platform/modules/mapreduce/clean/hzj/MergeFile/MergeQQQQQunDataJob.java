package org.platform.modules.mapreduce.clean.hzj.MergeFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 统计字段 并去重
 * 
 * @author 
 */
public class MergeQQQQQunDataJob {

	static FileSystem fs = null;
	static Configuration conf = new Configuration();
	static String regex = "^(correct)+.*$";
	// 输入/输出文件路径
	static String pathIn = "hdfs://192.168.0.10:9000/elasticsearch_clean_1/qq/qqqundata/20";
	static String pathOut = "hdfs://192.168.0.10:9000/elasticsearch_clean_1/qq/qqqundata/combine";

	public static void main(String[] args) throws Exception {
		Path path = new Path(pathIn);
		List<String> files = new ArrayList<>();
		HDFSUtils.readAllFiles(path, regex, files);
		files.add(pathOut);
		args = files.toArray(new String[0]);
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 创建job对象
		Job job = Job.getInstance(conf, "MergeQQQQQunDataJob");
		// 设置运行的类
		job.setJarByClass(MergeQQQQQunDataJob.class);
		// 设置mapper类
		job.setMapperClass(StatisticsFileMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 设置reducer类
		job.setReducerClass(StatisticsFileReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		// 设置输出的key和value值的类型

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

	static class StatisticsFileMap extends Mapper<LongWritable, Text, Text, Text> {
		private Gson gson;

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				String id = (String) original.get("_id");
				context.write(new Text(id), value);
			} catch (Exception e) {
			}
		}
	}

	static class StatisticsFileReduce extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text str : values) {
				context.write(NullWritable.get(), str);
				break;
			}
		}
	}
}
