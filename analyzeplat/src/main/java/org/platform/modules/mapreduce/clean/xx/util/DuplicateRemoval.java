package org.platform.modules.mapreduce.clean.xx.util;

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
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DuplicateRemoval extends Configured implements Tool{
public static class MyMap extends Mapper<LongWritable, Text, Text, Text>{
	public  Gson gson = null;
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				//金融
				if (original.containsKey("loanDate")) {
					original.put("loanDate", DateValidJob.FormatDate(original.get("loanDate").toString()));
				}
				if (original.containsKey("applyTime")) {
					original.put("applyTime", DateValidJob.FormatDate(original.get("applyTime").toString()));
				}
				if (original.containsKey("startTime")) {
					original.put("startTime", DateValidJob.FormatDate(original.get("startTime").toString()));
				}
				if (original.containsKey("endTime")) {
					original.put("endTime", DateValidJob.FormatDate(original.get("endTime").toString()));
				}
				//通讯录
				if (original.containsKey("time")) {
					original.put("time", DateValidJob.FormatDate(original.get("time").toString()));
				}
				//车主
				if (original.containsKey("registerTime")) {
					original.put("registerTime", DateValidJob.FormatDate(original.get("registerTime").toString()));
				}
				if (original.containsKey("licenseTime")) {
					original.put("licenseTime", DateValidJob.FormatDate(original.get("licenseTime").toString()));
				}
				if (original.containsKey("insuranceTime")) {
					original.put("insuranceTime", DateValidJob.FormatDate(original.get("insuranceTime").toString()));
				}
				if (original.containsKey("issueTime")) {
					original.put("issueTime", DateValidJob.FormatDate(original.get("issueTime").toString()));
				}
				/*if (original.containsKey("endTime")) {
					original.put("endTime", DateValidation.judgeDateFormat(original.get("endTime").toString()));
				}*/
				//航空 beginTime
				if (original.containsKey("beginTime")) {
					original.put("beginTime", DateValidJob.FormatDate(original.get("beginTime").toString()));
				}
				
				if(original.containsKey("_id")){
					String id = (String)original.get("_id");
					context.write(new Text(id), new Text(gson.toJson(original)));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	 }
	public static class MyReduce extends Reducer<Text, Text, Text, NullWritable>{
		public void reduce(Text key , Iterable<Text> value,Context context) throws IOException, InterruptedException{
			for(Text str:value){
				context.write(str, NullWritable.get());
				break;
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
//		conf.setLong("mapred.max.split.size",314572800);//300M
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "DuplicateRemovalAirplane");
		job.setJarByClass(DuplicateRemoval.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
//		job.setInputFormatClass(MyConbineFileInpuFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
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
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		args = new String[]{"hdfs://192.168.0.10:9000/warehouse_data/trip/airplane/","hdfs://192.168.0.10:9000/warehouse_clean/trip/airplane/"};
		if(args.length!=2){
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		try {
			HDFSUtils.readAllFiles(fss, null, list);
			list.add(args[1]);
			ToolRunner.run(new DuplicateRemoval(),list.toArray(new String[0]));
			System.exit(0);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
