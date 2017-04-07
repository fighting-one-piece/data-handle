package org.platform.modules.mapreduce.clean.hzj.HDFSESJob.V2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class QQQunDataHDFS2ESV2Job extends BaseHDFS2ESV2Job {
	
	public static void main(String[] args) {
		try {
			args = new String[] {"hdfs://192.168.0.10:9000/elasticsearch_clean_1/qq/qqqundata/20/"};
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0,len = files.size();i<len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 5 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "qq", "qqqundata", "cisiondata",
							"192.168.0.10,192.168.0.12","5000",sb.toString()};
					ToolRunner.run(new QQQunDataHDFS2ESV2Job(), args1);
					sb = new StringBuilder();
				}
				/*Thread.sleep(50);  //args[1]+i  
				System.out.println(files.get(i)+"	"+i);*/
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
