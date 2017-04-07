package org.platform.modules.mapreduce.clean.ly.hdfs2es.v2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class OtherBocaiHDFS2ESV2Job extends BaseHDFS2ESV2Job{
	public static void main(String[] args) {
//		args = new String[]{"hdfs://192.168.0.10:9000/elasticsearch_clean_1/bocai/correct-m-00006"};
		try {
			List<String> filePathList = new ArrayList<String>();
			String regex = "^(correct)+.*$";
			Path inputPath = new Path(args[0]);
			//遍历给定文件
			HDFSUtils.readAllFiles(inputPath, regex, filePathList);
			for (int i = 0, len = filePathList.size(); i < len; i++) {
					String[]  args1= new String[] { "other", "bocaioriginal", "cisiondata", 
							"192.168.0.10","8000", filePathList.get(i).toString()};
					System.out.println("下标： "+i);
					System.out.println("传入的路径--------->>>"+filePathList.get(i).toString());
					ToolRunner.run(new OtherBocaiHDFS2ESV2Job(),args1);
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
