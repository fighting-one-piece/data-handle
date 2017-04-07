package org.platform.modules.mapreduce.clean.ly.find;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 寻找地域性记录
 * 
 */
public class ConditionalSearchJob extends Configured implements Tool {

	public static class MyMap extends Mapper<LongWritable, Text, LongWritable, Text> {
		public Gson gson = null;
		// 手机地域号码段
		static List<String> phoneList = new ArrayList<String>();
		static {
			readSource(phoneList, "PhoneNumber");
		};
		// 身份证号码段
		static List<String> idCardList = new ArrayList<String>();
		static {
			readSource(idCardList, "IdCardNumber");
		};
		// 时间正则，判断记录属于哪个时间段
		String timeRegx = "";

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				boolean flag = true;
//				boolean flag2 = false;
				boolean flag3 = false;
				boolean flag4 = false;
//				if (original.containsKey("address")) {
//					String addressValue = (String) original.get("address");
//					if (matchAddress(addressValue)) {
//						context.write(key, value);
//						flag = false;
//					}
//				}
//				if (flag && original.containsKey("contactAddress")) {
//					String contactAddressValue = (String) original.get("contactAddress");
//					if (matchAddress(contactAddressValue)) {
//						context.write(key, value);
//						flag = false;
//					}
//				}
//				if (flag && original.containsKey("companyAddress")) {
//					String companyAddressValue = (String) original.get("companyAddress");
//					if (matchAddress(companyAddressValue)) {
//						context.write(key, value);
//						flag = false;
//					}
//				}
//				if (flag && original.containsKey("idCard")) {
//					String idCardValue = (String) original.get("idCard");
//					if (idCardList.contains(idCardValue.replaceAll("\\s", "").substring(0, 6))) {
//						flag2 = true;
//					}
//				}
				if (flag && original.containsKey("mobilePhone")) {
					String phoneValue = (String) original.get("mobilePhone");
					if (phoneList.contains(phoneValue.replaceAll("\\s", "").substring(0, 7))) {
						flag3 = true;
					}
				}
				if (flag && original.containsKey("telePhone")) {
					String callValue = (String) original.get("telePhone");
					if (phoneList.contains(callValue.replaceAll("\\s", "").substring(0, 7))) {
						flag4 = true;
					}
				}
//				if (flag && ((flag2 && flag3) || (flag2 && flag4))) {
//					context.write(key, value);
//				}
				if (flag3 || flag4) {
				context.write(key, value);
			}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class MyReduce extends Reducer<LongWritable, Text, Text, NullWritable> {
		public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for (Text text : value) {
				context.write(text, NullWritable.get());
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "ConditionalSearchJob");

		job.setJarByClass(ConditionalSearchJob.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
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

	public static boolean matchAddress(String address) {
		if (address == null || "".equals(address)) {
			return false;
		} else {
			Pattern p = Pattern.compile("^.*(成都).*$");
			Matcher m = p.matcher(address);
			return m.matches();
		}
	}

	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @return
	 */
	public static List<String> readSource(List<String> list, String fileName) {
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

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("路径不正确");
			System.exit(2);
		}
		String regex = "^(records-)+.*$";
		List<String> list = new ArrayList<String>();
		try {
			HDFSUtils.readAllFiles(args[0], regex, list);
			list.add(args[1]);
			System.out.println(list);
			ToolRunner.run(new ConditionalSearchJob(), list.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
