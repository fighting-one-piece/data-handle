package org.platform.modules.mapreduce.clean.ly.fileTool;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FindID extends Configured implements Tool{
	
public static class Map1 extends Mapper<LongWritable, Text ,Text,Text>{
		private	int count;
		private  TransportClient client;
		private Gson gson = null;
		private String index=null;
		private String type=null;
		
		//从ES查询
		public  List<Map<String, Object>> readIndexTypeDatasByQuery(String index, String type, QueryBuilder query) {
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
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			this.index="account";
			this.type="account";
			Settings settings = Settings.builder().put("cluster.name", "cisiondata")
					.put("client.tansport.sniff", true).build();
			client = TransportClient.builder().settings(settings).build();
			client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("192.168.0.10", 9300)));
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			count++;
			if(count%5000==0){
				@SuppressWarnings("unchecked")
				Map<String, Object> original = gson.fromJson(value.toString(), Map.class);
				String idValue = (String)original.get("_id");
				QueryBuilder query =QueryBuilders.termQuery("_id", idValue);
				List<Map<String, Object>> list  =readIndexTypeDatasByQuery(this.index,this.type,query);
				if(list.size()>0){
					String str1 = "存在";
					context.write(new Text(idValue), new Text(str1));
				}else{
					String str = "不存在";
					context.write(new Text(idValue), new Text(str));
				}
			}
		}
	 }
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key , Iterable<Text> value,Context context) throws IOException, InterruptedException{
			for(Text text : value){
					context.write(key, text);
			}
		}
}
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "FindId");
		job.setJarByClass(FindID.class);
		job.setMapperClass(Map1.class);
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
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		 args = new
				 String[]{"hdfs://192.168.0.115:9000/ly/account__034188c0_4165_4edd_b3eb_c3aba057b54e","hdfs://192.168.0.115:9000/test"};
				if(args.length!=2){
					System.out.println("参数不正确");
					System.exit(0);
				}
				List<String> filePathList = new ArrayList<String>();
				String regex = "^(correct)+.*$";
				HDFSUtils.readAllFiles(args[0], regex, filePathList);
				filePathList.add(args[1]);
					try {
						ToolRunner.run(new FindID(), filePathList.toArray(new String[0]));
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.exit(0);
			}
	}
