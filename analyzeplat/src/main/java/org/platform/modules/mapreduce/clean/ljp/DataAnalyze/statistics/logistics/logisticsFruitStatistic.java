package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics.logistics;

import java.io.IOException;
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
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 需求:1.顺丰物流数据 2.寄件地址:北京 3.收件地址:江苏,浙江,上海 4.物品类型:水果
 * 
 * @author Administrator
 *
 */
public class logisticsFruitStatistic extends Configured implements Tool {
	public static class Maps extends Mapper<LongWritable, Text, NullWritable, Text> {
		private static Logger LOG = LoggerFactory.getLogger(logisticsFruitStatistic.class);
		protected Gson gson = null;
		private static final String DELIMITER = "\\$#\\$";
		private static List<String> list = new ArrayList<String>();
		private static List<String> linkAddressList = new ArrayList<String>();
		private static List<String> addressList = new ArrayList<String>();
		static {
			list.add("originalLinkAddress");
			list.add("originalLinkRegion");
			list.add("destination");
			list.add("objectiveRegion");
			list.add("linkClientCode");
		}
		static {
			linkAddressList.add("linkAddress");
			linkAddressList.add("linkProvince");
			linkAddressList.add("linkCity");
			linkAddressList.add("originalLinkAddress");
			linkAddressList.add("originalLinkRegion");
			linkAddressList.add("linkCounty");
		}

		static {
			addressList.add("address");
			addressList.add("province");
			addressList.add("destination");
			addressList.add("objectiveRegion");
			addressList.add("county");
			addressList.add("city");
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
				boolean linkFlag = false;
				boolean addressFlag = false;
				boolean friutFlag = false;
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				for (String logisticsCompanyName : list) {
					if ((original.containsKey("logisticsCompanyName")
							&& ((String) original.get("logisticsCompanyName")).contains("顺丰"))
							|| (original.containsKey("sourceFile")
									&& ((String) original.get("sourceFile")).contains("顺丰"))
							|| original.containsKey((logisticsCompanyName))) {
						if (original.containsKey("commodity")) {
							String commodity = (String) original.get("commodity");
							List<String> friut = Arrays
									.asList(DataCleanUtils.indexReadSource("ljp/fruit.txt").split(DELIMITER));
							friutFlag = friut.contains(commodity) ? true : false;
						} else {
							friutFlag = false;
						}

						for (String link : linkAddressList) {
							if (original.containsKey(link)) {
								List<String> friutLinkAddress = Arrays.asList(
										DataCleanUtils.indexReadSource("ljp/fruitLinkAddress.txt").split(DELIMITER));
								String linkAddress = (String) original.get(link);
								if (friutLinkAddress.contains(linkAddress)) {
									linkFlag = true;
									break;
								} else {
									linkFlag = false;
								}
							} else {
								linkFlag = false;
							}
						}

						for (String receipt : addressList) {
							if (original.containsKey(receipt)) {
								List<String> friutAddress = Arrays.asList(
										DataCleanUtils.indexReadSource("ljp/fruitAddress.txt").split(DELIMITER));
								String address = (String) original.get(receipt);
								if (friutAddress.contains(address)) {
									addressFlag = true;
									break;
								} else {
									addressFlag = false;
								}
							} else {
								addressFlag = false;
							}
						}
					}
				}
				if (friutFlag && addressFlag && linkFlag)
					context.write(NullWritable.get(), new Text(gson.toJson(original)));

			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public static class Reduces extends Reducer<NullWritable, Text, NullWritable, Text> {
		public void reduce(NullWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			for (Text text : value) {
				context.write(NullWritable.get(), text);
			}
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
		Job job = Job.getInstance(conf, "logisticsFruitStatistic");
		job.setJarByClass(logisticsFruitStatistic.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduces.class);
		job.setNumReduceTasks(15);
		job.setMapOutputKeyClass(NullWritable.class);
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
			int ret = 0;
			args = new String[] { "hdfs://192.168.0.10:9000/warehouse_clean/financial/logistics/20170315_5/logistics__deeffd78_d0f5_40c5_ba26_915e5e33bf85",
					"hdfs://192.168.0.10:9000/warehouse_clean/financial/fruit2" };
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 10000 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] str = new String[] { sb.toString(), args[1] };
					System.out.println("i>>" + i);
					System.out.println("路径:>>" + sb.toString());
					ret = ToolRunner.run(new logisticsFruitStatistic(), str);
					sb = new StringBuilder();
				}
			}
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
