package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class HDFSFileStatisticsAbstract {

	public void StatisticNum(String StatisticsPhoneNumFilePath) {
		System.out.println("总条数为" + HDFSFileStatisticsMapper.readFileSum(StatisticsPhoneNumFilePath));
	}

	public void deleteFile(String deleteFilePath) {
		HDFSFileStatisticsMapper.deleteAll(new File(deleteFilePath));
	}

	public void createHDFSFile(String HDFSpath, String outputFile) {
		List<String> list = new ArrayList<String>();
		HDFSFileStatisticsMapper.hdfsFile(HDFSpath, list, outputFile);
	}
}
