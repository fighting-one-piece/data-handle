package org.platform.modules.mapreduce.clean.ly.findError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * 根据sourceFile，将想要的的DATA仓库中的数据输出到指定目录
 *
 */
public class RemoveTheSourceFile extends Configured implements Tool {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public Gson gson = null;
		// 放入错误的sourceFile记录
		static List<String> erroList = new ArrayList<String>();
		static {
			erroList.add("kuaidi_201501_201503_DATA_14389.csv");
			erroList.add("kuaidi_201501_201503_DATA_14394.csv");
			erroList.add("kuaidi_201501_201503_DATA_14401.csv");
			erroList.add("kuaidi_201501_201503_DATA_14405.csv");
			erroList.add("kuaidi_201501_201503_DATA_14430.csv");
			erroList.add("kuaidi_201501_201503_DATA_14436.csv");
			erroList.add("kuaidi_201501_201503_DATA_14440.csv");
			erroList.add("kuaidi_201501_201503_DATA_14443.csv");
			erroList.add("kuaidi_201501_201503_DATA_14445.csv");
			erroList.add("kuaidi_201501_201503_DATA_14497.csv");
			erroList.add("kuaidi_201501_201503_DATA_14386.csv");
			erroList.add("kuaidi_201501_201503_DATA_14388.csv");
			erroList.add("kuaidi_201501_201503_DATA_14392.csv");
			erroList.add("kuaidi_201501_201503_DATA_14403.csv");
			erroList.add("kuaidi_201501_201503_DATA_14437.csv");
			erroList.add("kuaidi_201501_201503_DATA_14393.csv");
			erroList.add("kuaidi_201501_201503_DATA_14427.csv");
			erroList.add("kuaidi_201501_201503_DATA25_14427.csv");
			erroList.add("kuaidi_201501_201503_DATA_14435.csv");
			erroList.add("kuaidi_201501_201503_DATA_14442.csv");
			erroList.add("kuaidi_201501_201503_DATA_14496.csv");
			erroList.add("kuaidi_201501_201503_DATA_14385.csv");
			erroList.add("kuaidi_201501_201503_DATA_14387.csv");
			erroList.add("kuaidi_201501_201503_DATA_14398.csv");
			erroList.add("kuaidi_201501_201503_DATA_14402.csv");
			erroList.add("kuaidi_201501_201503_DATA_14408.csv");
			erroList.add("kuaidi_201501_201503_DATA_14432.csv");
			erroList.add("kuaidi_201501_201503_DATA_14438.csv");
			erroList.add("kuaidi_201501_201503_DATA_14378.csv");
			erroList.add("kuaidi_201501_201503_DATA_14366.csv");
			erroList.add("快递_201510_201512_DATA_14325.csv");
			erroList.add("快递_201510_201512_DATA5_14333.csv");
			erroList.add("快递_201510_201512_DATA6_14342.csv");
			erroList.add("快递_201510_201512_DATA8_14353.csv");
			erroList.add("快递_201510_201512_DATA9_14358.csv");
			erroList.add("快递_201510_201512_DATA10_14363.csv");
			erroList.add("快递_201510_201512_DATA11_14368.csv");
			erroList.add("快递_201510_201512_DATA12_14373.csv");
			erroList.add("快递_201510_201512_DATA13_14377.csv");
			erroList.add("快递_201510_201512_DATA_14322.csv");
			erroList.add("快递_201510_201512_DATA_14326.csv");
			erroList.add("快递_201510_201512_DATA22_14345.csv");
			erroList.add("快递_201510_201512_DATA23_14348.csv");
			erroList.add("快递_201510_201512_DATA24_14352.csv");
			erroList.add("快递_201510_201512_DATA25_14356.csv");
			erroList.add("快递_201510_201512_DATA27_14365.csv");
			erroList.add("快递_201510_201512_DATA28_14367.csv");
			erroList.add("快递_201510_201512_DATA33_14323.csv");
			erroList.add("快递_201510_201512_DATA34_14327.csv");
			erroList.add("快递_201510_201512_DATA36_14335.csv");
			erroList.add("快递_201510_201512_DATA37_14346.csv");
			erroList.add("快递_201510_201512_DATA43_14372.csv");
			erroList.add("快递_201510_201512_DATA44_14376.csv");
			erroList.add("快递_201510_201512_DATA46_14312.csv");
			erroList.add("快递_201510_201512_DATA49_14328.csv");
			erroList.add("快递_201510_201512_DATA50_14332.csv");
			erroList.add("快递_201510_201512_DATA51_14336.csv");
			erroList.add("快递_201510_201512_DATA52_14347.csv");
			erroList.add("快递_201510_201512_DATA54_14355.csv");
			erroList.add("快递_201510_201512_DATA55_14357.csv");
			erroList.add("快递_201510_201512_DATA_14318.csv");
		}

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
				Map<String, Object> original = gson.fromJson(value.toString(),
						Map.class);
				Map<String, Object> map = CleanUtil.replaceSpace(original);
				if (map.containsKey("sourceFile")) {
					String sourceFile = (String) map.get("sourceFile");
					if (!erroList.contains(sourceFile)) {
						context.write(new Text("true"),value);
					}else{
						context.write(new Text(""),value);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {
		public MultipleOutputs<Text,NullWritable> multipleOutputs = null;
		public void setup(Context context) throws IOException,InterruptedException {  
			multipleOutputs = new MultipleOutputs<Text, NullWritable>(context); 
	     } 
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			if("true".equals(key.toString())){
				for(Text str:value){
					multipleOutputs.write(str, NullWritable.get(),"correct");
				}
			}else{
				for(Text str:value){
					multipleOutputs.write( str, NullWritable.get(),"incorrect");
				}
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = Job.getInstance(conf, "RemoveTheSourceFile");
		job.setJarByClass(RemoveTheSourceFile.class);
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

	
	public static List<String> readSource(String fileName) {
		InputStream in = null;
		BufferedReader br = null;
		List<String> list = new ArrayList<String>();
		try {
			in = RemoveTheSourceFile.class.getClassLoader().getResourceAsStream("logisticsFileName/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				list.add(line);
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
		return list;
	}
	
	
	public static void main(String[] args) {
//		args = new  String[]{"hdfs://192.168.0.115:9000/elasticsearch/financial/logistics","hdfs://192.168.0.115:9000/ly"};
		if(args.length!=2){
			System.out.println("请指定下标和输出目录");
			System.exit(0);
		}
		try {
			List<String> filePathList = RemoveTheSourceFile.readSource("fileName");
			int start = Integer.parseInt(args[0]);
			for (int i = start, len = filePathList.size(); i < len; i++) {
					String[] str = filePathList.get(i).split("/");
					String[]  args1 = new String[] {filePathList.get(i), args[1] + "/" +str[str.length-1]};
					System.out.println("执行文件--------------->>>>>>>>>>>>>" + str[str.length-1]);
					ToolRunner.run(new RemoveTheSourceFile(),args1);
					if(i!=0 && i%5==0){
						System.out.println("下标为：  "+  i);
						break;
					}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
