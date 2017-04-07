package org.platform.modules.mapreduce.clean.xx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class ContactES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String args[]) {
		
		args = new String[]{"192.168.0.105", "financial_new/finance", "hdfs://192.168.0.10:9000/elasticsearch_original/financial/finance/105","1500000"};
		try {
			int exitCode = ToolRunner.run(new ContactES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
