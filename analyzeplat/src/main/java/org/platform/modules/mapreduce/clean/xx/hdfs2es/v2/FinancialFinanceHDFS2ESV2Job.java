package org.platform.modules.mapreduce.clean.xx.hdfs2es.v2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class FinancialFinanceHDFS2ESV2Job extends BaseHDFS2ESV2Job {
	public static void main(String[] args) {
		try {
			args = new String[] { "hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial/finance/105" };
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 5 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "financial", "finance", "cisiondata", "192.168.0.10,192.168.0.12",
							"10000", sb.toString() };
					ToolRunner.run(new FinancialFinanceHDFS2ESV2Job(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
