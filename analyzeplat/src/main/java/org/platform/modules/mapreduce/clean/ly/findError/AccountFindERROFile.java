package org.platform.modules.mapreduce.clean.ly.findError;


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
import org.platform.modules.mapreduce.clean.ly.mergeFile.MyConbineFileInpuFormat;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AccountFindERROFile extends Configured implements Tool{
	
public static class Map1 extends Mapper<LongWritable, Text ,NullWritable,Text>{
	// 存在主要字段，但是不满足条件的value值
			static List<String> erroList = new ArrayList<String>();
			static {
				erroList.add("NA");
				erroList.add("NULL");
				erroList.add("");
				erroList.add("null");
			}
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}
		
		public  Gson gson = null;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
//				空白字符（空格，制表符等）
				String spaceRegex = "\\s+|(----){1}|(---){1}|(-----){1}";
				if(original.containsKey("password") && original.containsKey("email")){
					String email =  (String) original.get("email");
					String password =  (String) original.get("password");
					String[] str = email.split(spaceRegex);
					if(str.length == 2 && erroList.contains(password) && !erroList.contains(str[1])){
						original.put("email", str[0]);
						original.put("password", str[1]);
					}
				}
				// 特殊情况：如果一条记录里面有邮箱有密码，但是没有账号字段，则将邮箱字段放入账号字段
				if (original.containsKey("email") && original.containsKey("password")
						&& !original.containsKey("account")) {
					String email = (String) original.get("email");
					String password =  (String) original.get("password");
					if(!erroList.contains(email) && !erroList.contains(password) && CleanUtil.matchEmail(email)){
						original.put("account", email);
						original.remove("email");
						context.write(NullWritable.get(), new Text(gson.toJson(original)));
					}
				}
				if(original.containsKey("password") && original.containsKey("account")){
					String account =  (String) original.get("account");
					String password =  (String) original.get("password");
					String[] str = account.split(spaceRegex);
					if(str.length == 2 && erroList.contains(password) && !erroList.contains(str[1])){
						original.put("account", str[0]);
						original.put("password", str[1]);
						context.write(NullWritable.get(), new Text(gson.toJson(original)));
					}
				}
			} catch (Exception e) {
			}
		}
	 }
	
	public static class Reduce extends Reducer<NullWritable, Text, Text, NullWritable>{
		public void reduce(NullWritable key , Iterable<Text> value,Context context) throws IOException, InterruptedException{
				for(Text text : value){
					context.write(text, NullWritable.get());
				}
		}
}
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		conf.setLong("mapred.max.split.size",62914560);//60M
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "FindAccountERROFileJob");
		job.setJarByClass(AccountFindERROFile.class);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(MyConbineFileInpuFormat.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
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
//		args = new String[]{"hdfs://192.168.0.115:9000/elasticsearch_clean/account","hdfs://192.168.0.115:9000/ly"};
		if(args.length!=2){
			System.out.println("路径不正确");
			System.exit(2);
		}
		String regex = "^(incorrect)+.*$";
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		try {
			HDFSUtils.readAllFiles(fss, regex, list);
			list.add(args[1]);
			ToolRunner.run(new AccountFindERROFile(),list.toArray(new String[0]));
			System.exit(0);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
