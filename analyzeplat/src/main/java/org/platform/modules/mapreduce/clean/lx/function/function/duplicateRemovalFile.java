package org.platform.modules.mapreduce.clean.lx.function.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;
import org.platform.utils.JobUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 数据去重
 * 
 */
public class duplicateRemovalFile {

	static FileSystem fs = null;
	static Configuration conf = new Configuration();
	static String regex = "^(motherandbady)+.*$";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		String paths = "hdfs://192.168.0.10:9000/warehouse_data/work/motherandbady";
		String pathOut = "hdfs://192.168.0.10:9000/warehouse_data/work/motherandbady/s";

		List<String> files = new ArrayList<>();
		HDFSUtils.readAllFiles(paths, regex, files);
		files.add(pathOut);
		args = files.toArray(new String[0]); // 转换为string的数组格式
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName());
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // 运行时读取参数

		Job job = Job.getInstance(conf, "staticsMobilePhone");
		job.setJarByClass(duplicateRemovalFile.class);

		job.setMapperClass(duplicateRemovalMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(duplicateRemovalReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
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

	static class duplicateRemovalMap extends Mapper<LongWritable, Text, Text, Text> {
		// private IntWritable one = new IntWritable(1);
		private static Text line = new Text();
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
				
					/*处理字段时间格式*/
					if(original.containsKey("birthDay")){
						original.put("birthDay", DateValidJob.FormatDate(original.get("birthDay").toString()));
					}						
				
							line = new Text(gson.toJson(original));
							context.write(line, new Text());

//				if(original.containsKey("companyName")){
//					
//					line = new Text(original.get("companyName").toString());
//				context.write(line, new Text());				
//				}
			} catch (Exception e) {
			}
		}
	}
}

class duplicateRemovalReduce extends Reducer<Text, Text, Text, NullWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
