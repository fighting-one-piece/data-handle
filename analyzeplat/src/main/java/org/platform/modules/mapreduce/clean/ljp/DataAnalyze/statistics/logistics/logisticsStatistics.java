package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics.logistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class logisticsStatistics extends Configured implements Tool {

	private static Logger LOG = LoggerFactory.getLogger(logisticsStatistics.class);

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
				Map<String, Object> statistics = new HashMap<String, Object>();
				Map<String, Object> statisticsLink = new HashMap<String, Object>();

				String phone;
				String name;
				String address;
				String good;
				String endTime;
				String linkName;
				String linkPhone;
				String linkAddress;
				String begainTime;
				String orderDate;
				String orderTime;

				if (original.containsKey("phone")&&CleanUtil.matchPhone((String) original.get("phone"))) {
					phone = (String) original.get("phone");
				} else {
					phone = null;
				}

				if (original.containsKey("name") && (CleanUtil.matchChinese(((String) original.get("name")))
						|| CleanUtil.matchEnglish(((String) original.get("name"))))) {
					name = (String) original.get("name");
				} else {
					name = null;
				}
				if (original.containsKey("address") && CleanUtil.matchChinese((String) original.get("address"))) {
					address = (String) original.get("address");
				} else {
					address = null;
				}
				if (original.containsKey("good")) {
					good = (String) original.get("good");
				} else {
					good = null;
				}
				if (original.containsKey("endTime")) {
					endTime = (String) original.get("endTime");
				} else {
					endTime = null;
				}
				if (original.containsKey("linkName") && (CleanUtil.matchChinese(((String) original.get("linkName")))
						|| CleanUtil.matchEnglish(((String) original.get("linkName"))))) {
					linkName = (String) original.get("linkName");
				} else {
					linkName = null;
				}
				if (original.containsKey("linkPhone")&&CleanUtil.matchPhone((String) original.get("linkPhone"))) {
					linkPhone = (String) original.get("linkPhone");
				} else {
					linkPhone = null;
				}
				if (original.containsKey("linkAddress")
						&& CleanUtil.matchChinese((String) original.get("linkAddress"))) {
					linkAddress = (String) original.get("linkAddress");
				} else {
					linkAddress = null;

				}
				if (original.containsKey("begainTime")) {
					begainTime = (String) original.get("begainTime");
				} else {
					begainTime = null;
				}

				if (original.containsKey("orderDate") && !original.containsKey("begainTime")) {
					orderDate = (String) original.get("orderDate");
				} else {
					orderDate = null;
				}

				if (original.containsKey("orderTime")
						&& ("".equals((String) original.get("orderDate"))
								|| "NA".equals((String) original.get("orderDate")))
						&& !original.containsKey("begainTime")) {
					orderTime = (String) original.get("orderTime");
				} else {
					orderTime = null;
				}

				/**
				 * 收件人信息
				 */
				if (!("NA").equals(phone) && !("").equals(phone) && !("NA").equals(name) && !("").equals(name)
						&& !("NA").equals(address) && !("").equals(address) && !("NA").equals(good)
						&& !("").equals(good)) {
					if (phone != null)
						statistics.put("phone", phone);
					if (name != null)
						statistics.put("name", name);
					if (address != null) {
						statistics.put("address", address);
					}

					if (good != null)
						statistics.put("good", good);
					if (endTime != null) {
						statistics.put("endTime", endTime);
					}
					context.write(new Text(phone), new Text(gson.toJson(statistics)));
				}
				/**
				 * 寄件人信息
				 */
				if (!("NA").equals(linkName) && !("").equals(linkName) && !("NA").equals(linkPhone)
						&& !("").equals(linkPhone) && !("NA").equals((String) original.get("linkAddress"))
						&& !("").equals((String) original.get("linkAddress"))
						&& !("NA").equals((String) original.get("begainTime"))
						&& !("").equals((String) original.get("begainTime"))
						&& ((!("NA").equals((String) original.get("orderDate"))
								&& !("").equals((String) original.get("orderDate")))
								|| !(("NA").equals((String) original.get("orderTime"))
										&& !("").equals((String) original.get("orderTime"))))) {

					if (linkName != null) {
						statisticsLink.put("linkName", linkName);
					}
					if (linkPhone != null)
						statisticsLink.put("linkPhone", linkPhone);
					if (linkAddress != null) {
						statisticsLink.put("linkAddress", linkAddress);
					}

					if (begainTime != null) {
						statisticsLink.put("begainTime", begainTime);
					}

					if (orderDate != null) {
						statisticsLink.put("begainTime", orderDate);
					}

					if (orderTime != null) {
						statisticsLink.put("begainTime", orderTime);
					}

					if (good != null)
						statisticsLink.put("good", good);

					context.write(new Text(linkPhone), new Text(gson.toJson(statisticsLink)));
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

	/**
	 * 将读取的文件名存入到TXT文件中
	 * 
	 * @param path
	 *            读取文件的路径
	 * @param list
	 *            路径名的集合
	 * @param file
	 *            存储文件名的路径
	 */
	public static void hdfsFile(String path, List<String> list, String file) {
		String fss = path;
		Configuration conf = new Configuration();
		BufferedWriter bw = null;
		FileSystem hdfs;
		String rex = "^correct.*$";
		try {
			hdfs = FileSystem.get(URI.create(fss), conf); // 通过uri来指定要返回的文件系统
			FileStatus[] fs = hdfs.listStatus(new Path(fss)); // FileStatus
																// 封装了hdfs文件和目录的元数据
			Path[] listPath = FileUtil.stat2Paths(fs); // 将FileStatus对象转换成一组Path对象
			for (Path p : listPath) {
				String name = p.getName();
				if (name.matches(rex)) {
					list.add(p.toString());
					System.out.println(p.toString());
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
					for (String S : list) {
						bw.write(S.toString());
						bw.newLine();
					}
				} else if (name.startsWith("_SUCCESS") || name.startsWith("part-r-00000")
						|| name.startsWith("incorrect-m-")) {
					continue;
				} else {
					hdfsFile(p.toString(), list, file);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 从begin行开始，读取size行
	 * 
	 * @param begin
	 * @param size
	 * @param filePath
	 * @return
	 */
	public static List<String> readFileLineNum(int begin, int size, String filePath) {
		List<String> list = new ArrayList<String>();
		int nowRow = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
			String line = null;
			while ((line = br.readLine()) != null) {
				nowRow++;
				if (nowRow >= begin && nowRow < (begin + size)) {
					list.add(line);
				}
				if (nowRow >= (begin + size))
					break;
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}

	public int run(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "logisticsStatistics");
		job.setJarByClass(logisticsStatistics.class);
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
			long timeStar = System.currentTimeMillis();
			List<String> list = new ArrayList<String>();
			// args = new String[] {
			// "hdfs://192.168.0.115:9000/elasticsearch/Test/output/correct-m-00000",
			// "hdfs://192.168.0.115:9000/elasticsearch/Test/output3" };
			// hdfsFile("hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial_new/logistics/11_change/",
			// list,
			// "F:/Count/logistics_change_2.txt");
			list = readFileLineNum(Integer.valueOf(args[0]), Integer.valueOf(args[1]),
					"/home/ljp/DataName/logistics_change_2");
			String outputPath = args[2];
			args = new String[list.size() + 1];
			for (int i = 0; i < list.size(); i++) {
				args[i] = list.get(i);
			}
			args[list.size()] = outputPath;
			int ret = ToolRunner.run(new logisticsStatistics(), args);
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
