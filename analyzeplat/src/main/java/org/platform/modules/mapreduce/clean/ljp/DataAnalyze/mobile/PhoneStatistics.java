package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.mobile;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 统计去重后的电话号码!
 * 
 * @author Administrator
 *
 */
public class PhoneStatistics extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(PhoneStatistics.class);
	private static List<String> list = new ArrayList<String>();
	static {
		list.add("mobilePhone");
		list.add("telePhone");
		list.add("linkMobilePhone");
		list.add("linkTelePhone");
		list.add("idCode");
		list.add("linkClientCode");
	}

	public static class Maps extends Mapper<LongWritable, Text, Text, NullWritable> {
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
				boolean bool = false;
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				for (String phoneName : list) {
					if (original.containsKey(phoneName) && ((String) original.get(phoneName)).length() <= 11
							&& CleanUtil.allMatchPhone((String) original.get(phoneName))) {
						context.write(new Text((String) original.get(phoneName)), NullWritable.get());
					} else if (original.containsKey(phoneName) && ((String) original.get(phoneName)).length() >= 11
							&& CleanUtil.matchPhone((String) original.get(phoneName))) {
						if (((String) original.get(phoneName)).contains(",")) {
							String[] str = ((String) original.get(phoneName)).split(",");
							for (int i = 0; i < str.length; i++) {
								if (CleanUtil.matchPhone(str[i]))
									context.write(new Text(str[i]), NullWritable.get());
							}
						} else {
							bool = true;
						}
					}
					if (bool) {
						Pattern p = Pattern.compile(CleanUtil.phoneRex);
						Matcher m = p.matcher((String) original.get(phoneName));
						while (m.find()) {
							context.write(new Text(m.group()), NullWritable.get());
						}
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public static class Reduces extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> value, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName());
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("hadoop.job.user", "dataplat");
		// conf.set("mapreduce.job.queuename", "hdfs2es");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "statisticsLogisticsPhone");
		job.setJarByClass(PhoneStatistics.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduces.class);
		job.setNumReduceTasks(20);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
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
			// args = new String[] {
			// "hdfs://192.168.0.115:9000/user/ljp/logistics/input/correct-m-00000",
			// "hdfs://192.168.0.115:9000/user/ljp/logistics/output3" };
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(logistics)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 10000 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] str = new String[] { sb.toString(), args[1] };
					System.out.println("i>>" + i);
					System.out.println("路径:>>" + sb.toString());
					ret = ToolRunner.run(new PhoneStatistics(), str);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
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
