package org.platform.modules.mapreduce.clean.ly.fileTool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.platform.modules.mapreduce.clean.ly.find.ConditionalSearchJob;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FindRecordJob extends Configured implements Tool{
	public static class MyMap extends Mapper<LongWritable, Text, LongWritable, Text>{
		public  Gson gson = null;
		public static List<String> PhoneList= new ArrayList<String>();
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			PhoneList=readSource("phone.txt");
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				boolean flag=true;
				if(original.containsKey("linkMobilePhone")){
					if(PhoneList.contains(original.get("linkMobilePhone"))){
						flag=false;
						context.write(key, new Text(gson.toJson(original)));
					}
				}
				if(flag && original.containsKey("mobilePhone")){
					if(PhoneList.contains(original.get("mobilePhone"))){
						flag=false;
						context.write(key, new Text(gson.toJson(original)));
					}
				}
				if(flag &&original.containsKey("idCode")){
					if(PhoneList.contains(original.get("idCode"))){
						flag=false;
						context.write(key, new Text(gson.toJson(original)));
					}
				}
				if(flag &&original.containsKey("linkClientCode")){
					if(PhoneList.contains(original.get("linkClientCode"))){
						flag=false;
						context.write(key, new Text(gson.toJson(original)));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	public static class MyReduce extends Reducer<LongWritable, Text ,Text,NullWritable>{
		public void reduce(NullWritable key , Iterable<Text> value,Context context) throws IOException, InterruptedException{
			for(Text str : value){
				context.write(str,NullWritable.get());
			}
		}
	}
	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @return
	 */
	public static List<String> readSource(String fileName) {
		List<String> list = new ArrayList<String>();
		InputStream in = null;
		BufferedReader br = null;
		try {
			in = ConditionalSearchJob.class.getClassLoader().getResourceAsStream("mapping/ly/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				list.add(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != br) {
					br.close();
				}
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	

	@Override
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("mapreduce.job.queuename", "queue3");
		conf.set("hadoop.job.user", "dataplat");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "FindRecordJob");
		job.setJarByClass(FindRecordJob.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
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

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		try {
			HDFSUtils.readAllFiles(fss, "^((correct)|(incorrect))+.*$", list);
			list.add(args[1]);
			ToolRunner.run(new FindRecordJob(),
					list.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
