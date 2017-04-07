package org.platform.modules.mapreduce.clean.ly.hdfs2es.v2;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class AccountHDFS2ESV2Job extends BaseHDFS2ESV2Job{
	public static void main(String[] args) {
		  NumberFormat numberFormat = NumberFormat.getInstance();
	        // 设置精确到小数点后2位  
	      numberFormat.setMaximumFractionDigits(2); 
		try {
			List<String> filePathList = new ArrayList<String>();
			String regex = "^(correct)+.*$";
			Path inputPath = new Path(args[0]);
			//遍历给定文件
			HDFSUtils.readAllFiles(inputPath, regex, filePathList);
//			filePathList=RemoveTheSourceFile.readSource("fileName");
			for (int i = 0, len = filePathList.size(); i < len; i++) {
					String[]  args1= new String[] { "account", "account", "cisiondata", 
							"192.168.0.10","5000", filePathList.get(i).toString()};
					 String progress = numberFormat.format((float)( i+1) / (float) len * 100);
					System.out.println("下标： "+i+"			"+"当前进度 :"+ progress+"%");
					System.out.println("传入的路径--------->>>"+filePathList.get(i).toString());
					ToolRunner.run(new AccountHDFS2ESV2Job(),args1);
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
