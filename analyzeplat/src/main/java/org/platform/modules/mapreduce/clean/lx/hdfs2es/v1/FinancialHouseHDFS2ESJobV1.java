package org.platform.modules.mapreduce.clean.lx.hdfs2es.v1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class FinancialHouseHDFS2ESJobV1 extends BaseHDFS2ESV1Job{
	public static void main(String[] args) {
		try {
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 10 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "financial", "house", "cisiondata", "192.168.0.10,192.168.0.12",
							sb.toString(),args[1]};
					ToolRunner.run(new FinancialHouseHDFS2ESJobV1(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
