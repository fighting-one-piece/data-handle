package org.platform.modules.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.platform.utils.monitor.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseJob extends Configured implements Tool {
	
	protected Logger LOG = LoggerFactory.getLogger(getClass());
	
	//成功
	public static final int SUCCESS = 0;
	//失败
	public static final int FAILURE = 1;

	/** 获取JOB名称*/
	protected String getJobName() {
        return getClass().getSimpleName();
	}
	
	protected Configuration newConfiguration() {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false); 
		conf.setBoolean("mapreduce.reduce.speculative", false); 
		conf.set("mapreduce.job.queuename", JobUtils.getCurrentMinCapacityQueueName()); 
		conf.set("hadoop.job.user", "dataplat"); 
		return conf;
	}

}
