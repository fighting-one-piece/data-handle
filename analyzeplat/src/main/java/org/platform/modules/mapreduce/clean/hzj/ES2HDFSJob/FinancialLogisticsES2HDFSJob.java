package org.platform.modules.mapreduce.clean.hzj.ES2HDFSJob;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;

public class FinancialLogisticsES2HDFSJob extends BaseES2HDFSJob{
	public static void main(String[] args) {

		try {
			args = new String[]{"192.168.0.10","financial/logistics","hdfs://192.168.0.10:9000/elasticsearch_original/financial/logistics/10","1000000"};
			int exitCode = ToolRunner.run(new FinancialLogisticsES2HDFSJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}