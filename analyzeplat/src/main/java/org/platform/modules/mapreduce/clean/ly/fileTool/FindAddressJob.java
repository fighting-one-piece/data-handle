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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.ly.find.ConditionalSearchJob;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FindAddressJob extends Configured implements Tool {
	public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
		public Gson gson = null;
		public static List<String> addressList = new ArrayList<String>();
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
			addressList = readSource("肖家河.txt");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				// 判断寄件人和收件人的地址是否为肖家河地址，以及是否在成都
				boolean flagAddress = false;
				if (original.containsKey("address")) {
					// 判断寄件人、收件人城市地址是否为成都
					boolean flagCity = false;
					if (original.containsKey("city")) {
						String city = (String) original.get("city");
						if(city.indexOf("成都")>0){
							flagCity =true;
						}
					}
					
					boolean flagChengdu = false;
					String address = (String) original.get("address");
					//判读地址中是否包含“成都”
					if(address.indexOf("成都") > 0){
						flagChengdu=true;
					}
					for (String target : addressList) {
						if (address.indexOf(target) > 0) {
							flagAddress = true;
							break;
						}
					}
					if((flagChengdu && flagAddress) || (flagCity && flagAddress)){
						context.write(new Text("address"), value);
					}
				}
				
				if (!flagAddress && original.containsKey("linkAddress")) {
					// 判断寄件人、收件人城市地址是否为成都
					boolean flagCity = false;
					if (!flagCity && original.containsKey("linkCity")) {
						String linkCity = (String) original.get("linkCity");
						if(linkCity.indexOf("成都")>0){
							flagCity =true;
						}
					}
					boolean flagChengdu = false;
					String linkAddress = (String) original.get("linkAddress");
					//判读地址中是否包含“成都”
					if(linkAddress.indexOf("成都") > 0){
						flagChengdu=true;
					}
					for (String target : addressList) {
						if (linkAddress.indexOf(target) > 0) {
							flagAddress = true;
							break;
						}
					}
					
					if((flagChengdu && flagAddress) || (flagCity && flagAddress)){
						context.write(new Text("linkAddress"), value);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class MyReduce extends Reducer<Text, Text, Text, NullWritable> {
		public MultipleOutputs<Text, NullWritable> multipleOutputs = null;
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
		}
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			if(key.toString().equals("address")){
				for (Text str : value) {
					multipleOutputs.write(str, NullWritable.get(),"address");
				}
			}
			if(key.toString().equals("linkAddress")){
				for (Text str : value) {
					multipleOutputs.write(str, NullWritable.get(),"linkAddress");
				}
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

		job.setMapOutputKeyClass(Text.class);
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
		if (args.length != 1) {
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list1 = new ArrayList<String>();
		List<String> list2 = new ArrayList<String>();
		try {
			HDFSUtils.readAllFiles("/warehouse_clean/financial/logistics/20170315_5/logistics__deeffd78_d0f5_40c5_ba26_915e5e33bf85", "^(correct)+.*$", list1);
			HDFSUtils.readAllFiles("/warehouse_clean/financial/logistics/xlsx_20170315_new", "^(logistics)+.*$", list2);
			list1.addAll(list2);
			list1.add(args[0]);
			System.out.println(list1.size());
			ToolRunner.run(new FindAddressJob(), list1.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
