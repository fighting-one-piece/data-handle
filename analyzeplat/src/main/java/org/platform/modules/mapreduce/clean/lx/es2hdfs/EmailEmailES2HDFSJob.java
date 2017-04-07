package org.platform.modules.mapreduce.clean.lx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class EmailEmailES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String args[]) {		
		args = new String[]{"192.168.0.20", "email/mailbox", "hdfs://192.168.0.10:9000/elasticsearch_original/email/mailbox/20/","2500000"};
		try {
			int exitCode = ToolRunner.run(new EmailEmailES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
