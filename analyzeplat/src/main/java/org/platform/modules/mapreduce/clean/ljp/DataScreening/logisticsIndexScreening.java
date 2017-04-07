package org.platform.modules.mapreduce.clean.ljp.DataScreening;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class logisticsIndexScreening extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(logisticsIndexScreening.class);
	private static List<String> list = new ArrayList<String>();
	private static List<String> phoneList = null;
	static {
		list.add("mobilePhone");
		list.add("linkMobilePhone");
		list.add("linkClientCode");
		list.add("idCode");
	}

	public static class Maps extends Mapper<LongWritable, Text, Text, Text> {
		protected Gson gson = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			phoneList = Arrays.asList(readSource("phone").split(","));
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				for (String index : list) {
					if (original.containsKey(index)) {
						for (String phone : phoneList) {
							if (((String) original.get(index)).contains(phone))
								context.write(new Text((String) original.get("_id")), new Text(gson.toJson(original)));
						}
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public static class Reduces extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for (Text T : value) {
				context.write(NullWritable.get(), T);
				break;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		 args = new
		 String[]{"hdfs://192.168.0.10:9000/warehouse_clean/financial/logistics/xlsx_20170315_new"
		 ,"hdfs://192.168.0.10:9000/warehouse_clean/financial/logistics/xlsx_20170315_index"};
		List<String> files = new ArrayList<String>();
		HDFSUtils.readAllFiles(args[0], "^(logistics)+.*$", files);
		StringBuilder sb = new StringBuilder();
		for (int i = 0, len = files.size(); i < len; i++) {
			sb.append(files.get(i)).append(",");
			if ((i % 100000 == 0 || i == (len - 1)) && i != 0) {
				sb.deleteCharAt(sb.length() - 1);
				String[] str = new String[] { sb.toString(), args[1] + i };
				System.out.println("i>>" + i);
				System.out.println("路径:>>" + sb.toString());
				int exitCode = ToolRunner.run(new logisticsIndexScreening(), str);
				sb = new StringBuilder();
				System.exit(exitCode);
			}
		}
	}

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "queue3");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "indexScreening");
		job.setJarByClass(logisticsIndexScreening.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduces.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
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

	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @param fileName
	 * @return
	 */
	private static String readSource(String fileName) {
		InputStream in = null;
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = logisticsIndexScreening.class.getClassLoader().getResourceAsStream("mapping/ljp/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				sb.append(line).append(",");
			}
		} catch (Exception e) {
			LOG.error("read source error.", e);
		} finally {
			try {
				if (null != br) {
					br.close();
				}
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				LOG.error("close reader or stream error.", e);
			}
		}
		return sb.deleteCharAt(sb.length() - 1).toString();
	}
}
