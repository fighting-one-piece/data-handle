package org.platform.modules.mapreduce.clean.skm.Tool;

import java.io.IOException;
import java.util.ArrayList;
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
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;
import org.platform.utils.IDGenerator;
import org.platform.utils.JobUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 文件数据去重 格式化 日期
 * @author skm
 */
public class BusinessDuplicateRemovaIdJob extends Configured implements Tool {

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
				original = CleanUtil.replaceMap(original);
				if (original.containsKey("openDate")) {
					original.put("openDate", DateValidJob.FormatDate(original.get("openDate").toString()));
				}
				if (original.containsKey("setupDate")) {
					original.put("setupDate", DateValidJob.FormatDate(original.get("setupDate").toString()));
				}
				if (original.containsKey("beginTime")) {
					original.put("beginTime", DateValidJob.FormatDate(original.get("beginTime").toString()));
				}
				if (original.containsKey("endTime")) {
					original.put("endTime", DateValidJob.FormatDate(original.get("endTime").toString()));
				}
				if (original.containsKey("birthday")) {
					original.put("birthday", DateValidJob.FormatDate(original.get("birthday").toString()));
				}
				if (original.containsKey("onlineTime")) {
					original.put("onlineTime",
							DateValidJob.FormatDate(original.get("onlineTime").toString()));
				}

				if (original != null) {
					
					String id = IDGenerator.generateByMapValues(original, "inputPerson", "sourceFile", "updateTime",
							"insertTime", "_id","cnote");
					if (!"".equals(id) && id != null) {
						original.put("_id", id);
						context.write(new Text(id), new Text(gson.toJson(original)));
					}
				}
				//System.out.println(original);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class DuplicateRemovalRecordReducer extends Reducer<Text, Text, Text, NullWritable> {

		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for(Text str:value){
				context.write(str, NullWritable.get());
				break;
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("hadoop.job.user", "dataplat");
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName()); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "BusinessDuplicateRemovaIdJob");
		job.setJarByClass(BusinessDuplicateRemovaIdJob.class);

		job.setMapperClass(DuplicateRemovalRecordMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DuplicateRemovalRecordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(80);

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

		args = new String[] { "hdfs://192.168.0.10:9000/warehouse_data/financial/business",
				              "hdfs://192.168.0.10:9000/warehouse_data/financial/business_new" };
		if (args.length != 2) {
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list = new ArrayList<String>();
		String regex = "^(part)+.*$";
		try {
			HDFSUtils.readAllFiles(args[0], regex, list);
			list.add(args[1]);
			ToolRunner.run(new BusinessDuplicateRemovaIdJob(), list.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	

}
