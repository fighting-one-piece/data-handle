package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics.Date;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class logisticsDateformatStatistics  extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(logisticsDateformatStatistics.class);
	private static List<String> list = new ArrayList<String>();
	private static String str = null;
	private static IntWritable one = new IntWritable(1);
	static{
		list.add("begainTime");
		list.add("endTime");
		list.add("orderTime");
		list.add("pieceTime");
		list.add("orderDate");
		list.add("endDate");
	}
	public static class Maps extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected Gson gson = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				for(String dateName : list){
					if(original.containsKey(dateName)){
						str = ((String)original.get(dateName)).substring(0, 4);
						context.write(new Text(dateName+"_"+str),one);
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public static class Reduces extends Reducer<Text, IntWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			Long count = 0L;
			for (IntWritable intWritable : value) {
				count += intWritable.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
//		conf.set("mapreduce.job.queuename", "hdfs2hdfs");
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName());
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "logisticsDateformatStatistics");
		job.setJarByClass(logisticsDateformatStatistics.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduces.class);
		job.setMapOutputKeyClass(Text.class);
		job.setNumReduceTasks(10);
		job.setMapOutputValueClass(IntWritable.class);
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
	
	public static void main(String[] args) throws Exception {
		int ret = 0;
//		args = new String[]{"hdfs://192.168.0.115:9000/user/ljp/logistics/input/logisticsDateTest"
//				,"hdfs://192.168.0.115:9000/user/ljp/logistics/output2"};
		List<String> files = new ArrayList<String>();
		HDFSUtils.readAllFiles(args[0], "^(logistics)+.*$", files);
		StringBuilder sb = new StringBuilder();
		for (int i = 0, len = files.size(); i < len; i++) {
			sb.append(files.get(i)).append(",");
			if ((i % 10000 == 0 || i == (len - 1)) && i != 0) {
				sb.deleteCharAt(sb.length() - 1);
				String[] str = new String[] { sb.toString(), args[1] };
				System.out.println("i>>" + i);
				System.out.println("路径:>>" + sb.toString());
				ret = ToolRunner.run(new logisticsDateformatStatistics(), str);
				sb = new StringBuilder();
			}
		}
		System.exit(ret);
	}
}
