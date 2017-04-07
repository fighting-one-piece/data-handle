package org.platform.modules.mapreduce.clean.hzj.DuplicateRemoval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.DateFormatter;
import org.platform.utils.IDGenerator;
import org.platform.utils.JobUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DuplicateRemovalRecordV2Job extends Configured implements Tool {

	static class DuplicateRemovalRecordMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public Gson gson = null;

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				original = CleanUtil.replaceMap(original);		
				//判断数据中是否有 insertTime
				if (!original.containsKey("insertTime")) {
					original.put("insertTime", DateFormatter.TIME.get().format(new Date()));
				}
				String id = IDGenerator.generateByMapValues(original, "inputPerson","insertTime","sourceFile","updateTime","_id","cnote");
				original.put("_id", id);
				context.write(new Text(id), new Text(gson.toJson(original)));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class DuplicateRemovalRecordReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iterator = value.iterator();
			while (iterator.hasNext()) {
				context.write(new Text(iterator.next()), NullWritable.get());
				break;
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		/*conf.setBoolean("mapreduce.map.speculative", false); 
		conf.setBoolean("mapreduce.reduce.speculative", false); 
		conf.set("hadoop.job.user", "dataplat"); */
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName()); 
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "DuplicateHospitalRemovalV2Job");
		job.setJarByClass(DuplicateRemovalRecordV2Job.class);
		
		job.setMapperClass(DuplicateRemovalRecordMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DuplicateRemovalRecordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		/*更改reduce数量*/
		job.setNumReduceTasks(1);
		
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 0; i < (args_len - 1); i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(oArgs[args_len - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//args = new String[]{"hdfs://192.168.0.10:9000/warehouse_data/qq/qqqunrelation","hdfs://192.168.0.10:9000/warehouse_data/qq/qqqunrelation_new"};
		args = new String[]{"hdfs://192.168.0.10:9000/warehouse_data/work/hospital",
				"hdfs://192.168.0.10:9000/warehouse_clean/work/hospital"};
		if (args.length != 2) {
			System.out.println("路径不正确");
			System.exit(2);
		}
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		String regex = "^(hospital)+.*$";
		try {
			HDFSUtils.readAllFiles(fss, regex, list);
			list.add(args[1]);
			ToolRunner.run(new DuplicateRemovalRecordV2Job(), list.toArray(new String[0]));
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
