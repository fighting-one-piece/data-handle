package org.platform.modules.mapreduce.clean.lx.hdfs2es.v2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
/**
 * 公积金导入es
 * v2版本需要执行输入参数 
 * @author lixin
 */
public class WorkAccumulationFundHDFS2ESV2Job extends BaseHDFS2ESV2Job{
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		try {
			List<String> filePathList = new ArrayList<String>();
			String regex = "^(correct)+.*$";
			Path inputPath = new Path(args[0]);
			//遍历给定文件
			HDFSUtils.readAllFiles(inputPath, regex, filePathList);
			for (int i = 0, len = filePathList.size(); i < len; i++) {
					String[]  args1= new String[] { "work", "accumulationfund", "cisiondata", 
							"192.168.0.10","5000", filePathList.get(i).toString()};
					System.out.println("下标： "+i);
					System.out.println("传入的路径--------->>>"+filePathList.get(i).toString());
					ToolRunner.run(new WorkAccumulationFundHDFS2ESV2Job(),args1);
			}
			long endTime = System.currentTimeMillis();
			System.out.println("所用时间"+((endTime-startTime)/1000)+"s");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
