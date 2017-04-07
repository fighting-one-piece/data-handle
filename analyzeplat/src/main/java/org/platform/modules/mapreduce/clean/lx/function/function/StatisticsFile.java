package org.platform.modules.mapreduce.clean.lx.function.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * 统计字段，分析
 */
public class StatisticsFile {
	//需要统计的字段
	 static String fileName="idCard";
	//输入/输出文件路径
	static String paths = "hdfs://192.168.0.10:9000/warehouse_data/financial/car"; 
	// static String paths = "hdfs://192.168.0.115:9000/elasticsearch/other/bocai/k/"; 
	static	String pathOut ="hdfs://192.168.0.115:9000/elasticsearch/test/ls"+fileName;	
	static FileSystem fs = null;
	static Configuration conf = new Configuration();
	static String regex = "^(car_)+.*$";
	//static String regex = "^(records)+.*$";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {				
		Path path = new Path(paths);
		List<String> files = new ArrayList<>();
		HDFSUtils.readAllFiles(path, regex, files);
		files.add(pathOut);
		args = files.toArray(new String[0]);
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 创建job对象
		Job job = Job.getInstance(conf, "StatisticsFile");
		//设置运行的类
		job.setJarByClass(StatisticsFile.class);
		//设置mapper类
		job.setMapperClass(StatisticsFileMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置reducer类
		job.setReducerClass(StatisticsFileReduce.class);
		job.setNumReduceTasks(1);
		//设置输出的key和value值的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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
	 static class StatisticsFileMap extends Mapper<Object, Text, Text, Text>{
		private static Text line=new Text();
		private Gson gson;
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				//System.out.println(original.toString());
				if (original.containsKey(fileName)) {
					String file = (String) original.get(fileName);
					line = new Text(file);
					//if (!("NA").equals(mobilePhone) && !("").equals(mobilePhone) && CleanUtil.matchPhone(mobilePhone)) {
						context.write(line, new Text(""));
				//	}
				}
			} catch (Exception e) {
			}
		}
	}
	 static class StatisticsFileReduce extends Reducer<Text, Text, Text, Text>{		
		 public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			 context.write(key, new Text("")); 
	       }	 
	}
}

