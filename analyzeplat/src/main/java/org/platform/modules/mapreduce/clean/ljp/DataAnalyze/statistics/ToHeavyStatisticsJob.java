package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics;

import java.io.IOException;
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
import org.platform.utils.IDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ToHeavyStatisticsJob extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(ToHeavyStatisticsJob.class);
	public static class Maps extends Mapper<LongWritable, Text, Text, Text> {
		protected Gson gson = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				String[] excludeKeys = new String[] { "updateTime", "sourceFile", "insertTime", "inputPerson", "_id" };
				String id = IDGenerator.generateByMapValues(original, excludeKeys);
				context.write(new Text(id), new Text(gson.toJson(original)));
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

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "ToHeavyStatisticsJob");
		job.setJarByClass(ToHeavyStatisticsJob.class);
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

	public static void main(String[] args) throws Exception {
		args = new String[] { "hdfs://192.168.0.115:9000/user/ljpTest/input/toHeavy",
				"hdfs://192.168.0.115:9000/user/ljpTest/output" };
		int exitCode = ToolRunner.run(new ToHeavyStatisticsJob(), args);
		System.exit(exitCode);
	}

}
