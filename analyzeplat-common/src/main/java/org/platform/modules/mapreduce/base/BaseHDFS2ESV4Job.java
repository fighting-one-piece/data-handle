package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

/** HDFS数据文件批量导入ES */
public abstract class BaseHDFS2ESV4Job extends BaseJob {
	
	/**
	 * 获取Mapper类
	 * @return
	 */
	public Class<? extends BaseHDFS2ESV4Mapper> getMapperClass() {
		return BaseHDFS2ESV4Mapper.class;
	}
	
	/**
	 * 参数1：ES 集群IP
	 * 参数2：ES 集群端口
	 * 参数3：ES Index
	 * 参数4：ES Type
	 * 参数5：HDFS输入路径
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = newConfiguration();
		conf.set("es.nodes", args[0] + ":" + args[1]);  
        conf.set("es.resource", args[2] + "/" + args[3]);   
        conf.set("es.input.json", "yes");  
        conf.set("es.mapping.id", "id");  
		String[] oArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (oArgs.length < 5) {
			LOG.error("error! need 5 input parameters!");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, getJobName());
		job.setJarByClass(BaseHDFS2ESV4Job.class);
		job.setMapperClass(getMapperClass());
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(EsOutputFormat.class);
		
		int args_len = oArgs.length;
		StringBuilder inputPaths = new StringBuilder();
		for (int i = 4; i < args_len; i++) {
			inputPaths.append(oArgs[i]).append(",");
		}
		if (inputPaths.length() > 0) inputPaths.deleteCharAt(inputPaths.length() - 1);
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
		return job.waitForCompletion(true) ? SUCCESS : FAILURE;
	}

}
