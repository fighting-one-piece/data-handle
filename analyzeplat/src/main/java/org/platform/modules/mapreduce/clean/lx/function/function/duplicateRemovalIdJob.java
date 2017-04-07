package org.platform.modules.mapreduce.clean.lx.function.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.utils.IDGenerator;
import org.platform.utils.JobUtils;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class duplicateRemovalIdJob extends Configured implements Tool {

	static class DuplicateRemovalRecordMapper extends Mapper<LongWritable, Text, Text, Text> {

		public Gson gson = null;

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);

				System.out.println("生成id前:" + original);
				String id = IDGenerator.generateByMapValues(original, "updateTime", "sourceFile", "insertTime", "_id",
						"cnote", "inputPerson");
				if (id != null && !"".equals(id)) {
					original.put("_id", id);
					context.write(new Text(id), new Text(gson.toJson(original)));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class DuplicateRemovalRecordReducer extends Reducer<Text, Text, Text, NullWritable> {

		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator = value.iterator();
			while (iterator.hasNext()) {
				context.write(new Text(iterator.next()), NullWritable.get());
				break;
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false); // 关闭预测机制
																// //Speculative（预测）机制
		conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName());
		// conf.set("mapreduce.framework.name", "yarn");
		conf.set("hadoop.job.user", "dataplat");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "DuplicateRemovalAccumulationFundJob");
		job.setJarByClass(duplicateRemovalIdJob.class);

		job.setMapperClass(DuplicateRemovalRecordMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DuplicateRemovalRecordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
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

		List<String> list = new ArrayList<String>();
		String fss = "/warehouse_data/email/email";
		try {
			HDFSUtils.readAllFiles(fss, "", list);
			String fot = "/warehouse_data/email_news";
			list.add(fot);
			JobUtils.getCurrentMinCapacityQueueName();
			ToolRunner.run(new duplicateRemovalIdJob(), list.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
