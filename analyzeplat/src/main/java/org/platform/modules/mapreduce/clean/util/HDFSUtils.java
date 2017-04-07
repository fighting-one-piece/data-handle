package org.platform.modules.mapreduce.clean.util;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);
	
	public static final String DATA_WAREHOUSE = "hdfs://192.168.0.10:9000/";
	
	public static FileSystem getFileSystem() {
		try {
			return FileSystem.get(URI.create(DATA_WAREHOUSE), new Configuration());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}
	
	public static FileSystem getFileSystem(String hdfsPath) {
		try {
			return FileSystem.get(URI.create(hdfsPath), new Configuration());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}
	
	/**
	 * 读取输入路径下的所有文件，支持正则匹配，如果不需要正则匹配，设置为null即可
	 * @param inputPath 输入路径
	 * @param regex 正则表达式
	 * @param files 文件列表
	 */
	public static void readAllFiles(String inputPath, String regex, List<String> files) {
		if (StringUtils.isBlank(inputPath)) return;
		FileSystem fs = inputPath.startsWith("hdfs://") ? getFileSystem(inputPath) : getFileSystem();
		readAllFiles(fs, new Path(inputPath), regex, files);
	}
	
	/**
	 * 读取输入路径下的所有文件，支持正则匹配，如果不需要正则匹配，设置为null即可
	 * @param inputPath 输入路径
	 * @param regex 正则表达式
	 * @param files 文件列表
	 */
	public static void readAllFiles(Path inputPath, String regex, List<String> files) {
		FileSystem fs = getFileSystem();
		readAllFiles(fs, inputPath, regex, files);
	}
	
	/**
	 * 读取输入路径下的所有文件，支持正则匹配，如果不需要正则匹配，设置为null即可
	 * @param fs
	 * @param inputPath 输入路径
	 * @param regex 正则表达式
	 * @param files 文件列表
	 */
	public static void readAllFiles(FileSystem fs, Path inputPath, String regex, List<String> files) {
		try {
			if (!fs.exists(inputPath)) return;
			FileStatus[] fileStatuses = fs.listStatus(inputPath);
			for (int i = 0, len = fileStatuses.length; i < len; i++) {
				FileStatus fileStatus = fileStatuses[i];
				if (fileStatus.isDirectory()) {
					readAllFiles(fs, fileStatus.getPath(), regex, files);
				} else if (fileStatus.isFile()) {
					if (!StringUtils.isBlank(regex)) {
						String name = fileStatus.getPath().getName();
						if (!name.matches(regex)) continue;
					} 
					files.add(fileStatus.getPath().toString());
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
}
