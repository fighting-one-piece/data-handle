package org.platform.modules.mapreduce.clean.ly.mergeFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MergeFileJob extends Configured implements Tool {

	public static class MyMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}

		public Gson gson = null;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				String id = (String) original.get("_id");
				context.write(new Text(id), value);
			} catch (Exception e) {
			}
		}
	}

	public static class MyReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			for (Text text : value) {
				context.write(text, NullWritable.get());
			}
		}
	}
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
//		conf.setLong("mapreduce.input.fileinputformat.split.maxsize",268435456); //一个G
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "MergeFileJob");
		
		job.setJarByClass(MergeFileJob.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
//		job.setInputFormatClass(MyConbineFileInpuFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
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
	
	
	public static void main(String[] args){
//		 args = new
//		 String[]{"hdfs://192.168.0.115:9000/warehouse_clean/work/socialsecurity","hdfs://192.168.0.115:9000/warehouse_clean/work/end"};
		if(args.length!=2){
			System.out.println("参数不正确");
			System.exit(0);
		}
		List<String> filePathList = new ArrayList<String>();
		String regex = "^(correct)+.*$";
		HDFSUtils.readAllFiles(args[0], regex, filePathList);
		filePathList.add(args[1]);
		try {
			if(args.length>1){
				ToolRunner.run(new MergeFileJob(), filePathList.toArray(new String[0]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
			System.exit(0);
	}

}
