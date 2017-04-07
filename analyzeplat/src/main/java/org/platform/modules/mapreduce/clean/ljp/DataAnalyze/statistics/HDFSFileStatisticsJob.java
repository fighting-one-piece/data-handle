package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics;

import java.io.File;



public class HDFSFileStatisticsJob extends HDFSFileStatisticsAbstract {
	//统计文件行数
	public void StatisticNum() {
		super.StatisticNum("F:" + File.separator + "Count" + File.separator + "logisticsFile.txt");
	}
	//  删除文件
	// (需要删除的文件路径)
	public void deleteFile() {
		super.deleteFile("");
	}
	// 存放HDFS文件路径至本地文件中
	//(需读取的HDFS路径,存放HDFS文件名的文件路径)
	public void createHDFSFile() {
		super.createHDFSFile("", "");
	}
}
