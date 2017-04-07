package org.platform.modules.mapreduce.clean.hzj.Tool;

import java.io.IOException;

import org.platform.modules.mapreduce.clean.util.DataCleanUtils;

public class DetectionHDFSFile {
	public static void main(String[] args) {
		try {
			/*检测文件是否有错*/
			DataCleanUtils.detectionHDFSFile("hdfs://192.168.0.10:9000/warehouse_original/account/account/20161226",
					"account", "^(account).*$");
			/*随机抽查*/
			//DataCleanUtils.randomReadHDFSFile("hdfs://192.168.0.10:9000/warehouse_clean/account/20161226", 1000000);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
