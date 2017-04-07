package org.platform.modules.mapreduce.clean.ly.divideArea;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.dictionary.DictionaryFactory;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.platform.modules.mapreduce.clean.ly.find.ConditionalSearchJob;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ProvinceDivideArea extends Configured implements Tool {
	public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
		public Gson gson = null;
		public MultipleOutputs<Text, Text> multipleOutputs = null;

		public Map<String, String> provinceMap;
		public Map<String, String> cityToProvinceMap;
		public Map<String, String> countyToCityMap;

		// 统计map
		public Map<String, Long> countProvincetMap = new HashMap<String, Long>();
		public Map<String, Long> countCityMap = new HashMap<String, Long>();
		public long totalCount = 0;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			// 加载不分词字段
			WordConfTools.set("dic.path", "classpath:dic.txt ," + ProvinceDivideArea.class.getClassLoader()
					.getResource("mapping/ly/dic.txt").toString().replace("file:/", ""));
			DictionaryFactory.reload();

			// 加载省
			provinceMap = readSource("省.txt");
			// 加载市省对应的map，市为key
			cityToProvinceMap = readSource("市_省.txt");
			// 加载县市对应的map，县为key
			countyToCityMap = readSource("县_市.txt");
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			@SuppressWarnings("unchecked")
			Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
			totalCount++;
			countProvincetMap.put("totalCount", totalCount);
			countCityMap.put("totalCount", totalCount);
			if (original.containsKey("address")) {
				String addressValue = (String) original.get("address");
				// 分词，最大正向匹配
				List<Word> words = WordSegmenter.seg(addressValue, SegmentationAlgorithm.MaximumMatching);
				// 开关
				boolean flagProvince = true;
				boolean flagCity = true;
				for (Word word : words) {
					String str = word.toString().replace("省", "").replace("市", "").replace("县", "");
					// 判断省
					if (provinceMap.containsKey(str)) {
						flagProvince = false;
						if (countProvincetMap.containsKey(str)) {
							countProvincetMap.put(str, countProvincetMap.get(str) + 1L);
						}
						countProvincetMap.put(str, 1L);
					}
					// 判断市
					if (cityToProvinceMap.containsKey(str)) {
						flagCity = false;
						String provinceValue = cityToProvinceMap.get(str);
						if (countCityMap.containsKey(provinceValue + "	" + str)) {
							countCityMap.put(provinceValue + "	" + str, countCityMap.get(str) + 1L);
						}
						countCityMap.put(provinceValue + "	" + str, 1L);
						// 无省有市
						if (flagProvince) {
							if (countProvincetMap.containsKey(provinceValue)) {
								countProvincetMap.put(provinceValue, countProvincetMap.get(provinceValue) + 1L);
							}
							countProvincetMap.put(provinceValue, 1L);
							flagProvince = false;
						}
					}
					if (flagProvince == false && flagCity == false) {
						break;
					}
					// 根据县判断市,省
					if (flagCity && countyToCityMap.containsKey(str)) {
						String cityValue = countyToCityMap.get(str);
						String provinceValue = provinceMap.get(cityValue);
						// 添加到市map
						if (countCityMap.containsKey(provinceValue + "	" + cityValue)) {
							countCityMap.put(provinceValue + "	" + cityValue, countCityMap.get(str) + 1L);
						}
						countCityMap.put(provinceValue + "	" + cityValue, 1L);
						// 是否添加到省map
						if (flagProvince) {
							if (countProvincetMap.containsKey(provinceValue)) {
								countProvincetMap.put(provinceValue, countProvincetMap.get(provinceValue) + 1L);
							}
							countProvincetMap.put(provinceValue, 1L);
						}
						break;
					}
				}
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for (Entry<String, Long> entry : countProvincetMap.entrySet()) {
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
			for (Entry<String, Long> entry : countCityMap.entrySet()) {
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
		}
	}

	public static class MyReduce extends Reducer<Text, Text, Text, LongWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			Long count = 0L;
			for (Text str : value) {
				count += Long.parseLong(str.toString());
			}
			context.write(key, new LongWritable(count));
		}

	}

	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @return
	 */
	public static Map<String, String> readSource(String fileName) {
		Map<String, String> map = new HashMap<String, String>();
		InputStream in = null;
		BufferedReader br = null;
		try {
			in = ConditionalSearchJob.class.getClassLoader().getResourceAsStream("mapping/ly/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				String[] str = line.split("\t");
				if (str.length == 2) {
					map.put(str[1], str[0]);
				} else {
					map.put(str[0], "");
				}
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
		return map;
	}

	@Override
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("hadoop.job.user", "dataplat");
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "ProvinceDivideArea");
		job.setJarByClass(ProvinceDivideArea.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
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

	public static void main(String[] args) {
		Map<String, String> cityToProvinceMap = readSource("县_市.txt");
		System.out.println(cityToProvinceMap);
		System.out.println(cityToProvinceMap.get(""));
	}

}
