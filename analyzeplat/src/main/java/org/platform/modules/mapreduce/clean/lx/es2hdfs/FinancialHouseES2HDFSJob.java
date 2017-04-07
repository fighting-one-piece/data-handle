package org.platform.modules.mapreduce.clean.lx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class FinancialHouseES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String args[]) {		
		args = new String[]{"192.168.0.20", "work/qualification", "hdfs://192.168.0.10:9000/warehouse_original/work/qualification","1500000"};
		try {
			int exitCode = ToolRunner.run(new FinancialHouseES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
