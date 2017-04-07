package org.platform.modules.mapreduce.clean.ljp.DataAnalyze.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSFileStatisticsMapper {

	/**
	 * 将HDFS的路径名存入本地文件中
	 * 
	 * @param HDFSpath
	 *            HDFS的路径
	 * @param list
	 *            存放路径的集合
	 * @param outputFile
	 *            输出文件
	 */
	public static void hdfsFile(String HDFSpath, List<String> list, String outputFile) {
		String fss = HDFSpath;
		Configuration conf = new Configuration();
		BufferedWriter bw = null;
		FileSystem hdfs;
		String rex = "^part.*$";
		try {
			hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			for (Path p : listPath) {
				String name = p.getName();
				if (name.matches(rex)) {
					list.add(p.toString());
					System.out.println(p.toString());
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
					for (String S : list) {
						bw.write(S.toString());
						bw.newLine();
					}
				} else if (name.startsWith("_SUCCESS")) {
					continue;
				} else {
					hdfsFile(p.toString(), list, outputFile);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 读取存放HDFS路径的文件
	 * 
	 * @param begin
	 *            开始条数
	 * @param size
	 *            读取长度
	 * @param filePath
	 *            读取文件目录
	 * @return
	 */
	public static List<String> readFileLineNum(int begin, int size, String filePath) {
		List<String> list = new ArrayList<String>();
		int nowRow = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
			String line = null;
			while ((line = br.readLine()) != null) {
				nowRow++;
				if (nowRow >= begin && nowRow < (begin + size)) {
					list.add(line);
				}
				if (nowRow >= (begin + size))
					break;
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}

	/**
	 * 将HDFS上面的文件存入本地中
	 * 
	 * @param HDFSPath
	 *            HDFS路径
	 * @param localTempPath
	 *            本地路径
	 * @throws IOException
	 */
	public static void HDFS2Local(String HDFSPath, String localTempPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HDFSPath), conf);
		FSDataInputStream fsdi = fs.open(new Path(HDFSPath));
		OutputStream output = new FileOutputStream(localTempPath);
		IOUtils.copyBytes(fsdi, output, 4096, true);
	}

	/**
	 * 遍历文件夹,并将所有的数据整合在一起
	 * 
	 * @param inputpath
	 *            输入路径
	 * @param outputFile
	 *            输出路径
	 */
	public static void traverseFolder(String inputpath, String outputFile) {
		File file = new File(inputpath);
		BufferedReader br = null;
		BufferedWriter bw = null;
		if (file.exists()) {
			File[] files = file.listFiles();
			if (files.length == 0) {
				System.out.println("文件夹是空的!");
				return;
			} else {
				for (File inputFile : files) {
					if (inputFile.isDirectory()) {
						System.out.println("文件夹:" + inputFile.getAbsolutePath());
						traverseFolder(inputFile.getAbsolutePath(), outputFile);
					} else {
						try {
							System.out.println("文件:" + inputFile.getAbsolutePath());
							br = new BufferedReader(new FileReader(inputFile.getAbsolutePath()));
							bw = new BufferedWriter(new FileWriter(outputFile, true));
							String line = br.readLine();
							while (line != null) {
								bw.write(line);
								bw.newLine();
								line = br.readLine();
							}
							bw.flush();
							br.close();
							bw.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		} else {
			System.out.println("文件不存在!");
		}
	}

	/**
	 * 统计文件有多少行
	 * 
	 * @return
	 */
	public static long readFileSum(String path) {
		long sum = 0;
		try {
			FileInputStream inputStream;
			inputStream = new FileInputStream(path);

			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

			while ((bufferedReader.readLine()) != null) {
				sum++;
			}
			bufferedReader.close();
			inputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sum;
	}

	/**
	 * 删除指定路径下的文件
	 * 
	 * @param file
	 *            绝对路径
	 */
	public static void deleteAll(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} else {
			File[] files = file.listFiles();
			for (File f : files) {
				deleteAll(f);
				f.delete();
			}
		}
	}
	
//	public static void main(String[] args) throws IOException {
//		HDFS2Local( "hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial/logistics/11_statistics_2/1/part-r-00000", "F:/Count/logisticsFile.txt");
//		System.out.println("搞定...");
//	}
}
