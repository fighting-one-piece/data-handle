package org.platform.modules.mapreduce.clean.ljp.Merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.JobUtils;

/**
 * 合并文件
 * 
 * @author Administrator 修改参数: 1.输入路径 2.输出路径 3.正则表达式 4.输出的文件个数 5.Job名字的更改
 * 
 *
 */
public class MergeCombineJob extends Configured implements Tool {

	public static void main(String[] args) {
		try {
			// 输入/输出文件路径
			args = new String[] { "hdfs://192.168.0.10:9000/user/ljp/Date/x",
					"^(part)+.*$", "hdfs://192.168.0.10:9000/user/ljp/Date/x" };
			int exitCode = 0;
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], args[1], files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 10000 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] str = new String[] { sb.toString(), args[2] + i };
					System.out.println("i>>" + i);
					System.out.println("路径:>>" + sb.toString());
					exitCode = ToolRunner.run(new MergeCombineJob(), str);
					sb = new StringBuilder();
				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
//		conf.set("mapreduce.job.queuename", "hdfs2hdfs"); 
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName()); 
		conf.set("mapreduce.framework.name", "yarn");
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "MergelogisticsPhoneJob");
		job.setJarByClass(MergeCombineJob.class);
		job.setMapperClass(StatisticsFileMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(StatisticsFileReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		int args_len = args.length;

		StringBuilder inputPaths = new StringBuilder();
		for (int j = 0; j < (args_len - 1); j++) {
			inputPaths.append(args[j]).append(",");
		}
		if (inputPaths.length() > 0)
			inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(inputArgs[args_len - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class StatisticsFileMap extends Mapper<LongWritable, Text, Text, NullWritable> {
//		private Gson gson;
//		protected void setup(Context context) throws IOException, InterruptedException {
//			super.setup(context);
//			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
//					.create();
//		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
//				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
//				context.write(new Text((String)original.get("_id")), new Text(gson.toJson(original)));
				context.write(value, NullWritable.get());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class StatisticsFileReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
				context.write(key,NullWritable.get());
		}
	}
}
