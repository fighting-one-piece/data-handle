package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.toHeavy;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;
import org.platform.utils.IDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 去重并转换日期格式
 * 
 * @author Administrator
 *
 */
public class healthproductsToHeavy extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(healthproductsToHeavy.class);

	public static class Maps extends Mapper<LongWritable, Text, Text, Text> {
		protected Gson gson = null;
		private static List<String> list = new ArrayList<String>();
		static {
			list.add("begainTime");
			list.add("endTime");
			list.add("orderDate");
			list.add("orderTime");
		}

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
				for (String timeName : list) {
					if (original.containsKey(timeName)) {
						String timeNameData = (String) original.get(timeName);
						original.put(timeName, !"".equals(timeNameData) && timeNameData != null
								? DateValidJob.FormatDate(timeNameData) : "NA");
					}
				}
				if (original != null) {
					original = CleanUtil.replaceMap(original);
					String id = IDGenerator.generateByMapValues(original, "inputPerson", "sourceFile", "updateTime",
							"insertTime", "_id", "cnote");
						original.put("_id", id);
						context.write(new Text(id), new Text(gson.toJson(original)));
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

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("hadoop.job.user", "dataplat");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "toRepeatLogistics");
		job.setJarByClass(healthproductsToHeavy.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduces.class);
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

	public static void main(String[] args) {
		long timeStar = System.currentTimeMillis();
		try {
			int ret = 0;
			ret = ToolRunner.run(new healthproductsToHeavy(), args);
			// List<String> files = new ArrayList<String>();
			// HDFSUtils.readAllFiles(args[0], "^(healthproducts)+.*$", files);
			// StringBuilder sb = new StringBuilder();
			// for (int i = Integer.parseInt(args[2]), len = files.size(); i <
			// len; i++) {
			// sb.append(files.get(i)).append(",");
			// if ((i % Integer.parseInt(args[3]) == 0 || i == (len - 1)) && i
			// != 0) {
			// sb.deleteCharAt(sb.length() - 1);
			// String[] str = new String[] { sb.toString(), args[1] };
			// System.out.println("i>>" + i);
			// System.out.println("路径:>>" + sb.toString());
			// ret = ToolRunner.run(new healthproductsToHeavy(), str);
			// sb = new StringBuilder();
			// }
			// }
			System.exit(ret);
			long timeEnd = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
