package org.platform.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(JobUtils.class);
	
	public static YARNRunner getYarnRunner() {
		String hostAddress = getCurrentYarnHostAddress();
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.job.tracker", hostAddress + ":9001");
		conf.set("yarn.resourcemanager.address", hostAddress + ":8032");
		conf.set("yarn.resourcemanager.admin.address", hostAddress + ":8033");
		conf.set("yarn.resourcemanager.webapp.address", hostAddress + ":8088");
		conf.set("yarn.resourcemanager.scheduler.address", hostAddress + ":8030");
		conf.set("yarn.resourcemanager.resource-tracker.address", hostAddress + ":8031");
		conf.set("mapreduce.jobhistory.address", hostAddress + ":10020");
		return new YARNRunner(conf);
	}
	
	@SuppressWarnings("unused")
	public static void MRJobMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			JobStatus[] jobStatusArray = yarnRunner.getAllJobs();
			for (int i = 0, len = jobStatusArray.length; i < len; i++) {
				JobStatus jobStatus = jobStatusArray[i];
				Counters counters = yarnRunner.getJobCounters(jobStatus.getJobID());
//				if (!jobStatus.getState().name().equalsIgnoreCase("FAILED")) continue;
				LOG.info("job id: {}", jobStatus.getJobID());
				LOG.info("job name: {}", jobStatus.getJobName());
//				LOG.info("job user: {}", jobStatus.getUsername());
//				LOG.info("job queue: {}", jobStatus.getQueue());
//				LOG.info("job starttime: {}", jobStatus.getStartTime());
//				LOG.info("job finishtime: {}", jobStatus.getFinishTime());
				LOG.info("job state: {}", jobStatus.getState().name());
				LOG.info("job file: {}", jobStatus.getJobFile());
				LOG.info("job map progress: {}", jobStatus.getMapProgress());
				LOG.info("job reduce progress: {}", jobStatus.getReduceProgress());
				LOG.info("job scheduling info: {}", jobStatus.getSchedulingInfo());
				LOG.info("job failure info: {}", jobStatus.getFailureInfo());
//				LOG.info("job used memory: {}", jobStatus.getUsedMem());
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void TaskTrackerMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			TaskTrackerInfo[] taskTrackerInfos = yarnRunner.getActiveTrackers();
			for (int i = 0, len = taskTrackerInfos.length; i < len; i++) {
				TaskTrackerInfo taskTrackerInfo = taskTrackerInfos[i];
				LOG.info("task tracker name: {}", taskTrackerInfo.getTaskTrackerName());
				LOG.info("task tracker report: {}", taskTrackerInfo.getBlacklistReport());
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void TaskMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			JobStatus[] jobStatusArray = yarnRunner.getAllJobs();
			for (int i = 0, len = jobStatusArray.length; i < len; i++) {
				JobStatus jobStatus = jobStatusArray[i];
				if (!"FinancialLogisticsHDFS2ESV2Job".equalsIgnoreCase(
						jobStatus.getJobName())) continue;
				
				if ("SUCCEEDED".equalsIgnoreCase(jobStatus.getState().name())) continue;
				
				LOG.info("job id : {} name: {}", jobStatus.getJobID(), jobStatus.getJobName());
				TaskReport[] mapTaskReports = yarnRunner.getTaskReports(jobStatus.getJobID(), TaskType.MAP);
				for (int j = 0, jLen = mapTaskReports.length; j < jLen; j++) {
					TaskReport taskReport = mapTaskReports[j];
//					if (!"KILLED".equalsIgnoreCase(taskReport.getState())) continue;
					LOG.info("task id: {} state: {}", taskReport.getTaskId(), taskReport.getState());
					for (String diagno : taskReport.getDiagnostics()) {
						LOG.info("msg: {}" + diagno);
					}
				}
				TaskReport[] reduceTaskReports = yarnRunner.getTaskReports(jobStatus.getJobID(), TaskType.REDUCE);
				for (int k = 0, kLen = reduceTaskReports.length; k < kLen; k++) {
					TaskReport taskReport = reduceTaskReports[k];
//					if (!"KILLED".equalsIgnoreCase(taskReport.getState())) continue;
					LOG.info("task id: {} state: {}", taskReport.getTaskId(), taskReport.getState());
				}
				System.out.println("######");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static void CounterMonitor() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			JobStatus[] jobStatusArray = yarnRunner.getAllJobs();
			for (int i = 0, len = jobStatusArray.length; i < len; i++) {
				JobStatus jobStatus = jobStatusArray[i];
				LOG.info("job id: {}", jobStatus.getJobID());
				LOG.info("job name: {}", jobStatus.getJobName());
				Counters counters = yarnRunner.getJobCounters(jobStatus.getJobID());
				LOG.info("job counters: {}", counters.countCounters());
				Iterator<CounterGroup> iterator = counters.iterator();
				while (iterator.hasNext()) {
					CounterGroup counterGroup = iterator.next();
					LOG.info("counter group name: {}", counterGroup.getName());
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	@SuppressWarnings("unused")
	public static void JobStatistics() {
		try {
			YARNRunner yarnRunner = getYarnRunner();
			LOG.info("job history dir: {}", yarnRunner.getJobHistoryDir());
			String jobId = "job_1482820815021_4493";
			LogParams logParams = yarnRunner.getLogFileParams(JobID.forName(jobId), TaskAttemptID.forName("attempt_1482820815021_4493_m_000000_0"));
			LOG.info("log params owner: {}", logParams.getOwner());
			
			JobStatus jobStatus = yarnRunner.getJobStatus(JobID.forName(jobId));
			LOG.info("job file {}", jobStatus.getJobFile());
			LOG.info("job tracking url {}", jobStatus.getTrackingUrl());
			LOG.info("job schedule info {}", jobStatus.getSchedulingInfo());
			Counters jobCounters = yarnRunner.getJobCounters(JobID.forName(jobId));
			LOG.info("job counters {}", jobCounters);
			TaskReport[] mapTaskReports = yarnRunner.getTaskReports(jobStatus.getJobID(), TaskType.MAP);
			for (int j = 0, jLen = mapTaskReports.length; j < jLen; j++) {
				TaskReport taskReport = mapTaskReports[j];
				LOG.info("task id: {} state: {}", taskReport.getTaskId(), taskReport.getState());
				for (String diagno : taskReport.getDiagnostics()) {
//					LOG.info("msg: {}" + diagno);
				}
				Counters counters = taskReport.getTaskCounters();
				LOG.info("task counters: {}", counters);
				LOG.info("task total count: {}", counters.countCounters());
				Iterator<CounterGroup> iterator = counters.iterator();
				while (iterator.hasNext()) {
					CounterGroup counterGroup = iterator.next();
					LOG.info("counter group name: {}", counterGroup.getName());
					Iterator<Counter> counterIterator = counterGroup.iterator();
					while (counterIterator.hasNext()) {
						Counter counter = counterIterator.next();
//						LOG.info("counter name: {}", counter.getName());
					}
				}
			}
			
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public static String getCurrentMinCapacityQueueName() {
		String currentMinCapacityQueueName = null;
		try {
			YARNRunner yarnRunner = getYarnRunner();
			QueueInfo[] queueInfos = yarnRunner.getChildQueues("root");
			double currentMinCapacity = Double.MAX_VALUE;
			for (int i = 0, len = queueInfos.length; i < len; i++) {
				QueueInfo queueInfo = queueInfos[i];
				String queueName = queueInfo.getQueueName();
				if ("default".equalsIgnoreCase(queueName)) continue;
				String schedulingInfo = queueInfo.getSchedulingInfo();
				String[] kvs = schedulingInfo.split(",");
				for (int j = 0, jlen = kvs.length; j < jlen; j++) {
					String[] kv = kvs[j].split(":");
					if (kv[0].trim().indexOf("CurrentCapacity") == -1) continue;
					double currentValue = Double.parseDouble(kv[1]);
					System.out.println("currentQueue: " + queueName + " currentValue: " + currentValue);
					if (currentValue < currentMinCapacity) {
						currentMinCapacity = currentValue;
						currentMinCapacityQueueName = queueName;
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return currentMinCapacityQueueName;
	}
	
	public static String getCurrentYarnHostAddress() {
		String defaultHostAddress = "192.168.0.10";
		try {
        	InetAddress inetAddress = InetAddress.getLocalHost();
            String hostAddress = inetAddress.getHostAddress();
            if ("192.168.0.115".equals(hostAddress)) return hostAddress;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return defaultHostAddress;
        }
		return defaultHostAddress;
	}
	
	public static void main(String[] args) {
		String mac = "";  
        try {  
            Process p = new ProcessBuilder("ifconfig").start();  
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));  
            String line;  
            while ((line = br.readLine()) != null) {  
                Pattern pat = Pattern.compile("\\b\\w+:\\w+:\\w+:\\w+:\\w+:\\w+\\b");  
                Matcher mat= pat.matcher(line);  
                if(mat.find())  
                {  
                    mac=mat.group(0);  
                }  
            }  
            br.close();  
       }  
       catch (IOException e) {}  
       System.out.println("本机MAC地址为:\n"+mac);  
	}

}
