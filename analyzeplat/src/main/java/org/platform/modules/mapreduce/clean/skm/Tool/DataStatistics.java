package org.platform.modules.mapreduce.clean.skm.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * 
 *统计时间
 */
public class DataStatistics {

	static String paths = "hdfs://192.168.0.10:9000/warehouse_data/other/internet";
	static String pathOut = "hdfs://192.168.0.115:9000/skm1/internet";
	static String regex = "^(part-)+.*$";
	
	static FileSystem fs = null;
	static Configuration conf = new Configuration();
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		List<String> files = new ArrayList<>();
		HDFSUtils.readAllFiles(paths, regex, files);	
		files.add(pathOut);
		args = files.toArray(new String[0]);
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 创建job对象
		Job job = Job.getInstance(conf, "BusinessCount");
		// 设置运行的类
		job.setJarByClass(DataStatistics.class);
		// 设置mapper类
		job.setMapperClass(StatisticsFileMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置reducer类
		job.setReducerClass(StatisticsFileReduce.class);
	
		
		
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

	static class StatisticsFileMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	

		private Gson gson =null;

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				@SuppressWarnings("unchecked")			
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
			    List<String>list=new ArrayList<String>();
				if(original.containsKey("onlineTime")){
					String set=(String)original.get("onlineTime");
				     String set1=set.substring(0, 4);
					 String reg="^[0-9]*$";
					if(set1.matches(reg))
					  {list.add(set1);
					  }
					
					
				}
				for(String str:list){
					context.write(new Text(str),new IntWritable(1));
				}
				

			} catch (Exception e) {
			}	
		}
	}
	static class StatisticsFileReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		 IntWritable vt=new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}	
             vt.set(sum);
			context.write(key, vt);
		}
	}
}
