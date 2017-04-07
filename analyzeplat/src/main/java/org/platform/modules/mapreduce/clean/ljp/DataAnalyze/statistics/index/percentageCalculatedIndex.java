package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class percentageCalculatedIndex extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(percentageCalculatedIndex.class);

	public static class Maps extends Mapper<LongWritable, Text, Text, Text> {
		public Gson gson = null;
		public IntWritable one = new IntWritable(1);
		public long totalNum = 0;
		public Map<String, Long> map = new HashMap<String, Long>();
		static List<String> erroList = new ArrayList<String>();
		static {
			erroList.add("NA");
			erroList.add("NULL");
			erroList.add("");
			erroList.add("null");
		}

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			totalNum++;
			map.put("totalNum", totalNum);
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				Iterator<Entry<String, Object>> it = original.entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, Object> entry = it.next();
					if (erroList.contains((String) entry.getValue()))
						continue;
					if (map.containsKey(entry.getKey())) {
						map.put(entry.getKey(), map.get(entry.getKey()) + 1);
					} else {
						map.put(entry.getKey(), 1L);
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for (Entry<String, Long> entry : map.entrySet()) {
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			Long count = 0L;
			for (Text str : value) {
				count += Long.parseLong(str.toString());
			}
			context.write(key, new LongWritable(count));
		}

	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "telecomStatisticsIndex");
		job.setJarByClass(percentageCalculatedIndex.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
		try {
			int ret = 0;
			args = new String[]{"hdfs://192.168.0.10:9000/warehouse_data/work/healthproducts"
					,"hdfs://192.168.0.10:9000/warehouse_data/work/healthproducts_toheavy"};
			ret = ToolRunner.run(new percentageCalculatedIndex(), args);
//			List<String> files = new ArrayList<String>();
//			HDFSUtils.readAllFiles(args[0], "^(logistics)+.*$", files);
//			StringBuilder sb = new StringBuilder();
//			for (int i = 0, len = files.size(); i < len; i++) {
//				sb.append(files.get(i)).append(",");
//				if (i % 1000 == 0 || i == (len - 1) && i != 0) {
//					sb.deleteCharAt(sb.length() - 1);
//					String[] str = new String[] { sb.toString(), args[1] };
//					ret = ToolRunner.run(new percentageCalculatedIndex(), str);
//					System.out.println("i>>" + i);
//					System.out.println("路径:>>" + sb.toString());
//					sb = new StringBuilder();
//				}
//			}
			System.exit(ret);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
}
