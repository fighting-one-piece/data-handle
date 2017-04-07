package org.platform.modules.mapreduce.clean.xx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class FinanceES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String args[]) {
		
		args = new String[]{"192.168.0.20", "financial/finance", "hdfs://192.168.0.10:9000/elasticsearch_original/financial/finance/20","1500000"};
		try {
			int exitCode = ToolRunner.run(new FinanceES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
