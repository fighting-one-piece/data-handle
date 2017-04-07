package org.platform.modules.mapreduce.clean.ljp.HDFS2ES.V2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class HealthproductsHDFS2ESJob extends BaseHDFS2ESV2Job{

	public static void main(String[] args) {
		try {
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 2 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "work", "healthproducts", "cisiondata", "192.168.0.10,192.168.0.12",
							"2000", sb.toString()};
					System.out.println("i>>" + i);
					System.out.println("路径:>>" + sb.toString());
					ToolRunner.run(new HealthproductsHDFS2ESJob(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
