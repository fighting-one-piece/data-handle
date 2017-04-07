package org.platform.modules.mapreduce.clean.ly.hdfs2hdfs.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.clean.ly.cleanTool.BocaiTool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * 博彩第一次清洗
 *		 只涉及手机号码和邮箱字段
 */
public class OtherBocaiHDFS2HDFSV1job1 extends Configured implements Tool {
	
	public static class Map1 extends Mapper<LongWritable, Text, NullWritable, Text> {
		public Gson gson = null;
		public MultipleOutputs<NullWritable,Text> multipleOutputs = null;
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			multipleOutputs = new MultipleOutputs<NullWritable,Text>(context);
			this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
					.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				boolean flag = false;
				Map<String, Object> original = gson.fromJson(value.toString(),
						Map.class);
				//添加别名字段
				if(original.containsKey("name")){
					String nameValue= (String)original.get("name");
					original.put("nameAlias", nameValue);
				}
				//放入原有的集合
				Map<String, Object> map = new HashMap<String, Object>();
				map.putAll(original);
				// 替换掉空白字符
				BocaiTool.replaceSpace(original);
				//对余额处理
				if(original.containsKey("balance")){
					String balanceValue = (String) original.get("balance");
					if(CleanUtil.matchChinese(balanceValue) || CleanUtil.matchEnglish(balanceValue)){
						BocaiTool.addCnote(original, balanceValue,"balance");
						original.put("balance", "0");
					}
				}
				//判断QQ号
				if(original.containsKey("qqNum")){
					String qqValue = (String)original.get("qqNum");
					if(CleanUtil.matchQQ(qqValue)){
						String findQQ = BocaiTool.cleanFile(original, CleanUtil.QQRex, "qqNum", qqValue);
						if(findQQ!=null){
							BocaiTool.addCnote(original, (String) original.get("qqNum"),"qqNum");
							original.put("qqNum", findQQ);
						}
					}else{
						BocaiTool.addCnote(original,qqValue,"qqNum");
						original.put("qqNum", "NA");
					}
				}
				//对邮箱处理
				if(original.containsKey("email")){
					String emailValue = (String)original.get("email");
					if(CleanUtil.matchEmail(emailValue)){
						String email = BocaiTool.cleanFile(original, CleanUtil.emailRex,
								"email", emailValue);
						if (email != null) {
							BocaiTool.addCnote(original, (String) original.get("email"),"email");
							original.put("email", email);
							flag=true;
						}
					}else{
						//寻找邮箱的值
						String findEmail = BocaiTool.findValue(original,CleanUtil.emailRex);
						if(findEmail!=null){
							BocaiTool.addCnote(original, (String) original.get("email"),"email");
							original.put("email", findEmail);
							flag=true;
						}else{
							BocaiTool.addCnote(original,emailValue,"email");
							original.put("email", "NA");
						}
					}
				}else{
					//寻找邮箱的值
					String findEmail = BocaiTool.findValue(original,CleanUtil.emailRex);
					if(findEmail!=null){
						BocaiTool.addCnote(original, (String) original.get("email"),"email");
						original.put("email", findEmail);
						flag=true;
					}
				}
				//对手机号码字段做处理
				if (original.containsKey("mobilePhone")) {
					String mobilePhone = (String) original.get("mobilePhone");
					if (CleanUtil.matchPhone(mobilePhone)) {
						String phone = BocaiTool.cleanFile(original, CleanUtil.phoneRex,
								"mobilePhone", mobilePhone);
						if (phone != null) {
							BocaiTool.addCnote(original, (String) original.get("mobilePhone"),"mobilePhone");
							original.put("mobilePhone", phone);
							flag=true;
						}
					} else {
						// 寻找手机号码
						String findPhone = BocaiTool.findValue(original,CleanUtil.phoneRex);
						if(findPhone!=null){
							BocaiTool.addCnote(original, (String) original.get("mobilePhone"),"mobilePhone");
							original.put("mobilePhone", findPhone);
							flag=true;
						}else{
							if(CleanUtil.matchCall(mobilePhone)){
								flag=true;
							}else{
								BocaiTool.addCnote(original,mobilePhone,"mobilePhone");
								original.put("mobilePhone", "NA");	
							}
						}
					}
				} else {
					// 寻找手机号码
					String findPhone = BocaiTool.findValue(original,CleanUtil.phoneRex);
					if(findPhone!=null){
						BocaiTool.addCnote(original, (String) original.get("mobilePhone"),"mobilePhone");
						original.put("mobilePhone", findPhone);
						flag=true;
					}
				}
				if(flag){
					multipleOutputs.write(NullWritable.get(),new Text(gson.toJson(original)),"correct");
				}else{
					multipleOutputs.write(NullWritable.get(),new Text(gson.toJson(map)),"incorrect");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	


	public int run(String args[]) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		String[] oArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = Job.getInstance(conf, "BocaiPhoneAndEmail");
		job.setJarByClass(OtherBocaiHDFS2HDFSV1job1.class);
		job.setMapperClass(Map1.class);


		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
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
//		args = new  String[]{"hdfs://192.168.0.115:9000/elasticsearch/financial/logistics","hdfs://192.168.0.115:9000/ly"};
		if(args.length!=2){
			System.exit(0);
		}
		String regex = "^(records)+.*$";
		List<String> list = new ArrayList<String>();
		String fss = args[0];
		try {
			HDFSUtils.readAllFiles(fss, regex, list);
			System.out.println(list);
			list.add(args[1]);
			ToolRunner.run(new OtherBocaiHDFS2HDFSV1job1(),list.toArray(new String[0]));
			System.exit(0);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
