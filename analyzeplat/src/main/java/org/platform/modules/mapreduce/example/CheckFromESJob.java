package org.platform.modules.mapreduce.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 读取HDFS文件,查询是否成功导入ES 修改参数:1.输入路径 2.输出路径 3.每隔多少条信息收集一条查询 4.集群的名字,IP,与需要查询的字段与类型
 * 
 * @author Administrator
 *
 */
public class CheckFromESJob extends Configured implements Tool {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		List<String> filePathList = new ArrayList<String>();
		int exitCode = 0;
		try {
			args = new String[] { "hdfs://192.168.0.10:9000/warehouse_clean/account/20161219/account__00c5e40c_f053_431f_b431_acae987ac16a/correct-m-00000,hdfs://192.168.0.10:9000/warehouse_clean/account/20161221/account__00547c5c_d69f_4de3_9a0c_23ced27d9a13/correct-m-00000",
					"hdfs://192.168.0.10:9000/warehouse_clean/account/check" };
//			if (args.length != 2) {
//				System.out.println("参数不正确");
//				System.exit(0);
//			}
//			String regex = "^(records)+.*$";
//			HDFSUtils.readAllFiles(args[0], regex, filePathList);
//			filePathList.add(args[1]);filePathList.toArray(new String[0])
			exitCode = ToolRunner.run(new CheckFromESJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

	public static class Maps extends Mapper<LongWritable, Text, Text, Text> {
		private int count;
		private TransportClient client;
		private Gson gson = null;
		private String index = null;
		private String type = null;

		public List<Map<String, Object>> readIndexTypeDatasByQuery(String index, String type, QueryBuilder query) {
			SearchResponse response = client.prepareSearch(index).setTypes(type).setQuery(query)
					.setSearchType(SearchType.QUERY_AND_FETCH).setScroll(new TimeValue(60000)).setSize(100)
					.setExplain(false).execute().actionGet();
			List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
			while (true) {
				SearchHit[] hitArray = response.getHits().getHits();
				for (int i = 0, len = hitArray.length; i < len; i++) {
					datas.add(hitArray[i].getSource());
				}
				if (hitArray.length == 0)
					break;
				response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute()
						.actionGet();
			}
			return datas;
		}

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().setDateFormat("yyyy-MM-dd HH:mm:ss")
					.create();
			this.index = "account";
			this.type = "account";
			Settings settings = Settings.builder().put("cluster.name", "cisiondata").put("client.tansport.sniff", true)
					.build();
			client = TransportClient.builder().settings(settings).build();
			client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("192.168.0.10", 9300)));
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count++;
			// 每5000条收取一条信息查询
			if (count % 5000 == 0) {
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				String id = (String) original.get("_id");
				QueryBuilder query = QueryBuilders.termQuery("_id", id);
				List<Map<String, Object>> list = readIndexTypeDatasByQuery(this.index, this.type, query);
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String filename = fileSplit.getPath().toString();
				context.write(new Text(filename), new Text(list.size() > 0 ? "存在--" + filename : "不存在--" + filename));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for (Text text : value) {
				context.write(NullWritable.get(), text);
				break;
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "HDFSQueryESJob");
		job.setJarByClass(CheckFromESJob.class);
		job.setMapperClass(Maps.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

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

}
