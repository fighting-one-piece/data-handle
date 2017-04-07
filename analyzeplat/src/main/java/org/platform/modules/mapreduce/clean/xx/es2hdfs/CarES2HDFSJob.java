package org.platform.modules.mapreduce.clean.xx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class CarES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String args[]) {
		
		args = new String[]{"192.168.0.10", "financial/car", "hdfs://192.168.0.10:9000/elasticsearch_original/financial/car","1500000"};
		try {
			int exitCode = ToolRunner.run(new CarES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
