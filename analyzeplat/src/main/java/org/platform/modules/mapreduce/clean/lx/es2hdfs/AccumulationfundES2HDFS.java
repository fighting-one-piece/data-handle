package org.platform.modules.mapreduce.clean.lx.es2hdfs;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class AccumulationfundES2HDFS extends BaseES2HDFSJob {
public static void main(String args[]) {
		args = new String[]{"192.168.0.10", "work/accumulationfund", "hdfs://192.168.0.10:9000/warehouse_data/work/AccumulationFund/s","30000000"};
		try {
			int exitCode = ToolRunner.run(new AccumulationfundES2HDFS(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
