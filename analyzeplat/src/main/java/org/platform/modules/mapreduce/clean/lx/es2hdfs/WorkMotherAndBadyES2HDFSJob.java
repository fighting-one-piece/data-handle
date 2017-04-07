package org.platform.modules.mapreduce.clean.lx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class WorkMotherAndBadyES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String args[]) {		
		args = new String[]{"192.168.0.20", "work/motherandbady", "hdfs://192.168.0.115:9000/elasticsearch/other/motherandbady/","2000000"};
		try {
			int exitCode = ToolRunner.run(new WorkMotherAndBadyES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
