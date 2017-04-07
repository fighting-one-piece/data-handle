package org.platform.modules.mapreduce.clean.ly.hdfs2es.v2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;

public class AccountjdHDFS2ESV2 extends BaseHDFS2ESV2Job{
	public static void main(String[] args) {
		args = new String[]{"hdfs://192.168.0.115:9000/ly/out/part-r-00000"};
		long startTime = System.currentTimeMillis();
		try {
			List<String> filePathList = new ArrayList<String>();
			filePathList.add(args[0]);
			for (int i = 0, len = filePathList.size(); i < len; i++) {
					String[]  args1= new String[] { "account", "accountjd", "cisiondata", 
							"192.168.0.114","5000", filePathList.get(i).toString()};
					System.out.println("下标： "+i);
					System.out.println("传入的路径--------->>>"+filePathList.get(i).toString());
					ToolRunner.run(new AccountjdHDFS2ESV2(),args1);
			}
			long endTime = System.currentTimeMillis();
			System.out.println("所用时间"+((endTime-startTime)/1000)+"s");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
