package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.idcard;

import java.io.IOException;
import java.util.HashMap;
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
import org.platform.modules.mapreduce.clean.util.DateValidation.DateValidJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 统计去重后的身份证号码
 * 
 * @author Administrator
 *
 */
public class idCardStatistics extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(idCardStatistics.class);

	public static class Maps extends Mapper<LongWritable, Text, Text, Text> {
		protected Gson gson = null;
		protected String endTime = null,begainTime = null,orderDate = null,orderTime = null;

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
				Map<String, Object> date = new HashMap<String, Object>();
				if (original.containsKey("endTime")) {
					endTime = (String) original.get("endTime");
					date.put("endTime", DateValidJob.FormatDate(endTime));
				}
				if (original.containsKey("begainTime")) {
					begainTime = (String) original.get("begainTime");
					date.put("begainTime", DateValidJob.FormatDate(begainTime));
				}
				if (original.containsKey("orderDate")) {
					orderDate = (String) original.get("orderDate");
					date.put("orderDate", DateValidJob.FormatDate(orderDate));
				}
				if (original.containsKey("orderTime")) {
					orderTime = (String) original.get("orderTime");
					date.put("orderTime", DateValidJob.FormatDate(orderTime));
				}
				context.write(new Text(""), new Text(gson.toJson(date)));
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public static class Reduces extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for(Text T : value){
				context.write(NullWritable.get(), T);
			}
		}
	}

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "judgeDateFormat");
		job.setJarByClass(idCardStatistics.class);
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

	public static void main(String[] args) {
		try {
//			long timeStar = System.currentTimeMillis();
//			args = new String[] { "hdfs://192.168.0.115:9000/user/ljpTest/input", "hdfs://192.168.0.115:9000/user/ljpTest/output" };
			int ret = 0;
			ret = ToolRunner.run(new idCardStatistics(), args);
//			List<String> files = new ArrayList<String>();
//			HDFSUtils.readAllFiles(args[0], "^(logistics)+.*$", files);
//			StringBuilder sb = new StringBuilder();
//			for (int i = 2, len = files.size(); i < len; i++) {
//				sb.append(files.get(i)).append(",");
//				if ((i % 1 == 0 || i == (len - 1)) && i != 0) {
//					sb.deleteCharAt(sb.length() - 1);
//					String[] str = new String[] { sb.toString(), args[1]+i };
//					System.out.println("i>>" + i);
//					System.out.println("路径:>>" + sb.toString());
//					ret = ToolRunner.run(new idCardStatistics(), str);
//					sb = new StringBuilder();
//					if(i>=2){
//						break;
//					}
//				}
//			}
//			System.exit(0);
//			long timeEnd = System.currentTimeMillis();
//			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
//			Date date = new Date(timeEnd - timeStar);
//			System.out.println("用时--->" + formatter.format(date));
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
