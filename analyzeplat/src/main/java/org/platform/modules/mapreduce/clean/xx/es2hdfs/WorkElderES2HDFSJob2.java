package org.platform.modules.mapreduce.clean.xx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class WorkElderES2HDFSJob2 extends BaseES2HDFSJob{
	public static void main(String args[]) {
		
		args = new String[]{"192.168.0.20", "work/elder", "hdfs://192.168.0.115:9000/user/xx/elder/","1500000"};
		try {
			int exitCode = ToolRunner.run(new WorkElderES2HDFSJob2(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
