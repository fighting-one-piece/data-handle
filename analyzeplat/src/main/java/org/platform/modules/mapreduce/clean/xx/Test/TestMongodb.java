package org.platform.modules.mapreduce.clean.xx.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.Document;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.utils.IDGenerator;
import org.platform.utils.JobUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TestMongodb {
	private static void configureJob(Job job) {
		job.setJarByClass(TestMongodb.class);
		// 设置mapper类
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置reducer类
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Gson gson;
		private MongoClient mongoClient = null;
		private MongoCollection<Document> collection = null;
		private static final String DELIMITER = "\\$#\\$";

		// 用来解决无法把json格式的数据转换为MAP
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
			ServerAddress serverAddress = new ServerAddress("192.168.0.20", 27018);
			mongoClient = new MongoClient(serverAddress);
			MongoDatabase database = mongoClient.getDatabase("test");
			collection = database.getCollection("test");
		}

		@SuppressWarnings("unchecked")
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);

				String province = null, city = null, county = null, address = null, linkProvince = null,
						linkCity = null, linkCounty = null, linkAddress = null;
				boolean flag = true;
				List<String> provinceList = Arrays
						.asList(DataCleanUtils.indexReadSource("ljp/province.txt").split(DELIMITER));
				List<String> cityList = Arrays.asList(DataCleanUtils.indexReadSource("ljp/city.txt").split(DELIMITER));
				List<String> countyList = Arrays
						.asList(DataCleanUtils.indexReadSource("ljp/county.txt").split(DELIMITER));

				if (original.containsKey("address") && !CleanUtil.matchNum(address) && !"".equals(address)
						&& (String) original.get("address") != null) {
					address = (String) original.get("address");
				}
				if (original.containsKey("linkAddress") && !CleanUtil.matchNum(linkAddress)
						&& !"".equals((String) original.get("linkAddress"))
						&& (String) original.get("linkAddress") != null) {
					linkAddress = (String) original.get("linkAddress");
				}

				for (int i = 0; i < provinceList.size(); i++) {
					if (original.containsKey("address") && address.contains(provinceList.get(i))) {
						province = provinceList.get(i) + "省";
						break;
					} else if (original.containsKey("province") && province == null) {
						String prov = original.get("province").toString();
						if (prov.contains(provinceList.get(i)) && !"".equals(prov) && prov != null
								&& !CleanUtil.matchNum(prov)) {
							province = (String) original.get("province");
						}
					}
				}
				for (int i = 0; i < provinceList.size(); i++) {
					if (original.containsKey("linkAddress") && linkAddress.contains(provinceList.get(i))) {
						linkProvince = provinceList.get(i) + "省";
						break;
					} else if (original.containsKey("linkProvince") && linkProvince == null) {
						String linkProv = original.get("linkProvince").toString();
						if (linkProv.contains(provinceList.get(i)) && !"".equals(linkProv) && linkProv != null
								&& !CleanUtil.matchNum(linkProv)) {
							linkProvince = (String) original.get("linkProvince");
						}
					}
				}

				for (int i = 0; i < cityList.size(); i++) {
					if (original.containsKey("address") && address.contains(cityList.get(i))) {
						city = cityList.get(i) + "市";
						break;
					} else if (original.containsKey("city") && city == null) {
						String cit = original.get("city").toString();
						if (cit.contains(cityList.get(i)) && !"".equals(cit) && cit != null
								&& !CleanUtil.matchNum(cit)) {
							city = (String) original.get("city");
						}
					}
				}
				for (int i = 0; i < cityList.size(); i++) {
					if (original.containsKey("linkAddress") && linkAddress.contains(cityList.get(i))) {
						linkCity = cityList.get(i) + "市";
						break;
					} else if (original.containsKey("linkCity") && linkCity == null) {
						String linkCit = original.get("linkCity").toString();
						if (linkCit.contains(cityList.get(i)) && !"".equals(linkCit) && linkCit != null
								&& !CleanUtil.matchNum(linkCit)) {
							linkCity = (String) original.get("linkCity");
						}
					}
				}
				for (int i = 0; i < countyList.size(); i++) {
					if (original.containsKey("county")
							&& (((String) original.get("county")).contains(countyList.get(i))
									&& !"".equals((String) original.get("county"))
									&& (String) original.get("county") != null)
							&& !CleanUtil.matchNum((String) original.get("county"))) {
						county = (String) original.get("county");
					} else if (original.containsKey("address") && address.contains(countyList.get(i))) {
						if (address.indexOf("区") > -1) {
							if (countyList.get(i).indexOf("区") > -1 && address.substring(0, address.indexOf("区") + 1)
									.indexOf(countyList.get(i)) != -1) {
								county = countyList.get(i);
								break;
							}
						} else {
							county = countyList.get(i);
						}
					}
					if (!"".equals(county) && null != county) {
						break;
					}
				}
				for (int i = 0; i < countyList.size(); i++) {
					if (original.containsKey("linkAddress") && (linkAddress).contains(countyList.get(i))) {
						if (linkAddress.indexOf("区") > -1) {
							if (countyList.get(i).indexOf("区") > -1 && linkAddress
									.substring(0, linkAddress.indexOf("区") + 1).indexOf(countyList.get(i)) != -1) {
								linkCounty = countyList.get(i);
								break;
							}
						} else {
							linkCounty = countyList.get(i);
						}
					} else if (original.containsKey("linkCounty")
							&& (((String) original.get("linkCounty")).contains(countyList.get(i))
									&& !"".equals((String) original.get("linkCounty"))
									&& (String) original.get("linkCounty") != null)
							&& !CleanUtil.matchNum((String) original.get("linkCounty"))) {
						linkCounty = (String) original.get("linkCounty");
					}
					if (!"".equals(linkCounty) && null != linkCounty) {
						break;
					}
				}

				if ((province == null || "".equals(province)) && (city == null || "".equals(city))
						&& (county == null || "".equals(county)) && (address == null || "".equals(address))
						&& (linkProvince == null || "".equals(linkProvince))
						&& (linkCity == null || "".equals(linkCity)) && (linkCounty == null || "".equals(linkCounty))
						&& (linkAddress == null || "".equals(linkAddress))) {
					flag = false;
				}
				if (flag) {
					List<Document> documents = new ArrayList<Document>();
					Document document1 = new Document();
					document1.put("province", province);
					document1.put("city", city);
					document1.put("county", county);
					document1.put("address", address);
					String id = IDGenerator.generateByMapValues(document1);
					document1.put("_id", id);
					documents.add(document1);
					Document document2 = new Document();
					document2.put("province", linkProvince);
					document2.put("city", linkCity);
					document2.put("county", linkCounty);
					document2.put("address", linkAddress);
					String linkId = IDGenerator.generateByMapValues(document2);
					document2.put("_id", linkId);
					documents.add(document2);
					collection.insertMany(documents);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName());
		args = new String[] { "hdfs://192.168.0.115:9000/user/xx/test/logistics",
				"hdfs://192.168.0.115:9000/user/xx/wordcount/logistics/" };
		String[] inputArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (inputArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

		Job job = Job.getInstance(conf, "TestMongodb");
		FileInputFormat.addInputPath(job, new Path(inputArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(inputArgs[1]));

		configureJob(job);
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}
