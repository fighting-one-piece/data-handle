package org.platform.modules.mapreduce.clean.hzj.Statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.JobUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class StatisticsMobilePhone extends Configured implements Tool {
	public static class MyMap extends Mapper<LongWritable, Text, Text, LongWritable> {
		public Gson gson = null;
		

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) {
			long totalCount = 0;
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				if (original.containsKey("mobilePhone")) {
					String mobilePhone = (String) original.get("mobilePhone");
					List<String> list = splitPhone(mobilePhone);
					for (String phone : list) {
						totalCount++;
						context.write(new Text(phone), new LongWritable(1));
					}
				}
				if (original.containsKey("telePhone")) {
					String telePhone = (String) original.get("telePhone");
					List<String> list = splitPhone(telePhone);
					for (String phone : list) {
						totalCount++;
						context.write(new Text(phone), new LongWritable(1));
					}
				}
				context.write(new Text("totalCount"), new LongWritable(totalCount));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static List<String> splitPhone(String phoneValue) {
		List<String> list = new ArrayList<String>();
		if (phoneValue.indexOf(",") > 0) {
			String[] str = phoneValue.split(",");
			for (String sd : str) {
				if (CleanUtil.matchPhone(sd))
					list.add(sd);
			}
		} else {
			if (CleanUtil.matchPhone(phoneValue))
				list.add(phoneValue);
		}
		return list;
	}

	public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> value, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable str : value) {
				count += Long.parseLong(str.toString());
			}
			context.write(key, new LongWritable(count));
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName()); 
		// conf.setLong("mapred.max.split.size",314572800);//300M
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "StatisticsMobilePhone");
		job.setJarByClass(StatisticsMobilePhone.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		// job.setInputFormatClass(MyConbineFileInpuFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(1);

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

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "hdfs://192.168.0.10:9000/warehouse_data/work/qualification",
		"hdfs://192.168.0.115:9000/elasticsearch/work/qualification" };
		if (args.length != 2) {
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		try {
			HDFSUtils.readAllFiles(fss, null, list);
			list.add(args[1]);
			ToolRunner.run(new StatisticsMobilePhone(), list.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
