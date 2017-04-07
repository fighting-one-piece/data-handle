package org.platform.modules.mapreduce.clean.xx.hdfs2es.v1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class FinancialFinanceHDFS2ESV1Job extends BaseHDFS2ESV1Job {
	public static void main(String[] args) {
		try {
			args = new String[] { "hdfs://192.168.0.10:9000/elasticsearch_original/financial/finance/clean/",
					"hdfs://192.168.0.10:9000/elasticsearch_original/financial/finance/back/" };
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 5 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "financial", "finance", "cisiondata", "192.168.0.10,192.168.0.12",
							"10000", sb.toString(), args[1] + i };
					ToolRunner.run(new FinancialFinanceHDFS2ESV1Job(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
