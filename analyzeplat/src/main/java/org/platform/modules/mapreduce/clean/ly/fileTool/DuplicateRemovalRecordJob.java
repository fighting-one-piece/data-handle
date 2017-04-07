package org.platform.modules.mapreduce.clean.ly.fileTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.platform.utils.DateFormatter;
import org.platform.utils.IDGenerator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 生成id后去重
 *
 */
public class DuplicateRemovalRecordJob extends Configured implements Tool{
	public static class MyMap extends Mapper<LongWritable, Text, Text, Text>{
		public  Gson gson = null;
		public static List<String> timeList= new ArrayList<String>();
		static {
			timeList.add("beginTime"); //student 入学时间
			timeList.add("endTime");  //student 毕业时间
			timeList.add("entranceTime"); //resum  入学时间
			timeList.add("registeredTime"); 	//博彩 注册时间
			timeList.add("loginTime");	//博彩	登录时间
			timeList.add("newDate");	//博彩	新增日期
		};
		
		public void setup(Context context) throws IOException, InterruptedException {
				super.setup(context);
				this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
						.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			}
			@SuppressWarnings("unchecked")
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
				try {
					Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
					replaceSpace(original);
					//日期格式的转换
					for(String fileName: timeList){
						if(original.containsKey(fileName)){
							String timeValue = (String)original.get(fileName);
							original.put(fileName, DateValidJob.FormatDate(timeValue));
						}
					}
					if (!original.containsKey("insertTime")) {
						original.put("insertTime", DateFormatter.TIME.get().format(new Date()));
					}
					//删除值为空的key
					CleanUtil.replaceMap(original);
					//重新生成id，跳过特殊的字段
					if(original!=null){
						String id = IDGenerator.generateByMapValues(original, "inputPerson","insertTime","sourceFile","updateTime","_id","cnote");
						original.put("_id", id);
						context.write(new Text(id), new Text(gson.toJson(original)));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		 }
		public static class MyReduce extends Reducer<Text, Text, Text, NullWritable>{
			public void reduce(Text key , Iterable<Text> value,Context context) throws IOException, InterruptedException{
				for(Text str : value){
					context.write(str, NullWritable.get());
					break;
				}
			}
		}

		public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.map.speculative", false); 
			conf.setBoolean("mapreduce.reduce.speculative", false); 
			conf.set("hadoop.job.user", "dataplat"); 
			String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			Job job = Job.getInstance(conf, "DuplicateRemovalRecordJob");
			job.setJarByClass(DuplicateRemovalRecordJob.class);
			
			job.setMapperClass(MyMap.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setNumReduceTasks(6);
			job.setReducerClass(MyReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			
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
		
		/**
		 * 将一个map的所有value中的空格去除
		 * 
		 * @param map
		 * @return
		 */
		public static Map<String, Object> replaceSpace(Map<String, Object> map) {
			Iterator<Entry<String, Object>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Object> entry = it.next();
				if ("insertTime".equals(entry.getKey()) || "updateTime".equals(entry.getKey())
						|| "registeredTime".equals(entry.getKey()) || "loginTime".equals(entry.getKey())
						|| "newDate".equals(entry.getKey()) 
						|| "birthDay".equals(entry.getKey()) 
						|| "beginTime".equals(entry.getKey()) 
						|| "endTime".equals(entry.getKey()) 
						|| "entranceTime".equals(entry.getKey())
						|| "registeredTime".equals(entry.getKey()))
					continue;
				entry.setValue((String.valueOf(entry.getValue())).replaceAll("\\s", "").replaceAll("", ""));
			}
			return map;
		}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
//			args = new String[]{"hdfs://192.168.0.115:9000/ly","hdfs://192.168.0.115:9000/ly/out"};
			if(args.length!=2){
				System.out.println("路径不正确");
				System.exit(2);
			}
			List<String> list = new ArrayList<String>();
			String regex = "^(bocai)+.*$";
			try {
				HDFSUtils.readAllFiles(args[0], regex, list);
				list.add(args[1]);
				ToolRunner.run(new DuplicateRemovalRecordJob(),list.toArray(new String[0]));
				System.exit(0);
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
}
