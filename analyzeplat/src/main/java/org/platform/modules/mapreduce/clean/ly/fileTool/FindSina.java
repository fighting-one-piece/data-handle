package org.platform.modules.mapreduce.clean.ly.fileTool;



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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * 找出同时存在新浪账号和密码的记录，并保存
 */
public class FindSina extends Configured implements Tool{
	public static class Map1 extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}
		
		public  Gson gson = null;
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				if(original.containsKey("loginTime")){
					String time = (String)original.get("loginTime");
					context.write(new Text(time), NullWritable.get());
				}else if(original.containsKey("newDate")){
					String time = (String)original.get("newDate");
					context.write(new Text(time), NullWritable.get());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	 }
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "FindDate");
		job.setJarByClass(FindSina.class);
		
		job.setMapperClass(Map1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
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
		args = new String[]{"hdfs://192.168.0.10:9000/warehouse_data/other/bocaioriginal/bocai__3c516023_e90e_40ad_9d0f_3cb431c79c40","hdfs://192.168.0.10:9000/elasticsearch_original/ly/test"};
		if(args.length!=2){
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		list.add(fss);
		try {
			list.add(args[1]);
			ToolRunner.run(new FindSina(),list.toArray(new String[0]));
			System.exit(0);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
