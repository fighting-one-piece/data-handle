package org.platform.modules.mapreduce.clean.ly.fileTool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FindAccountJD extends Configured implements Tool {

	public static class Map1 extends Mapper<LongWritable, Text, NullWritable, Text> {
		public Gson gson = null;
		public MultipleOutputs<NullWritable,Text> multipleOutputs = null;
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			multipleOutputs = new MultipleOutputs<NullWritable,Text>(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(),
						Map.class);
				int times=0;
				if(original.containsKey("idCard") && CleanUtil.matchIdCard((String)original.get("idCard"))){
					times++;
				}
				if(original.containsKey("mobilePhone") && CleanUtil.matchPhone((String)original.get("mobilePhone"))){
					times++;
				}
				if(original.containsKey("email") && CleanUtil.matchEmail((String)original.get("email"))){
					times++;
				}
				if(original.containsKey("name") && CleanUtil.matchChinese((String)original.get("name"))){
					times++;
				}
				if(original.containsKey("password") && CleanUtil.matchNumAndLetter((String)original.get("password"))){
					times++;
				}
				if(times==5){
					multipleOutputs.write(NullWritable.get(),new Text(gson.toJson(original)),"correct");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	


	public int run(String args[]) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		String[] oArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = Job.getInstance(conf, "FindAccountJD");
		job.setJarByClass(FindAccountJD.class);
		job.setMapperClass(Map1.class);


		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
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
	
	
	// 根据传入的路径读取hadoop文件的第一行，并指定分隔符,返回一个字符串
			public static String readFirstLine(FileSystem hdfs, Path path)
					throws IOException {
				BufferedReader br = null;
				@SuppressWarnings("unused")
				String line = null;
				try {
					FSDataInputStream fin = hdfs.open(path);
					br = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
					if ((line = br.readLine()) != null) {
						return br.readLine();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (br != null) {
						br.close();
					}
				}
				return null;
			}
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String path = "hdfs://192.168.0.10:9000/warehouse_original/account/account";
		FileSystem fs = FileSystem.get(URI.create(path),conf);
		String regex = "^(account)+.*$";
		List<String> list = new ArrayList<String>();
		try {
			HDFSUtils.readAllFiles(path, regex, list);
			for(String str :list){
				String sd = readFirstLine(fs,new Path(str));
				if(sd.indexOf("SGK02_dbo_sgk_114.txt")>0){
					System.out.println(str);
					System.out.println(sd);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}
