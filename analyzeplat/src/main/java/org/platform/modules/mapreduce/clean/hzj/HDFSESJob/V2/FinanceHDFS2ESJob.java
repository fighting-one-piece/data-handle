package org.platform.modules.mapreduce.clean.hzj.HDFSESJob.V2;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;

public class FinanceHDFS2ESJob extends BaseHDFS2ESV2Job{

	public static void main(String[] args) {
		try {

			int exitCode = 0;
			String n;

			for (int j = 2; j <= 7; j++) {
				for (int i = 0; i <= 9; i++) {
					if (i < 10) {
						n = "0" + i;
					} else {
						n = "" + i;
					}

					String file = "hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial_new/finance/20/records-"+j 
							+ "-m-000" + n;
					Configuration conf = new Configuration();
					FileSystem fs = FileSystem.get(URI.create(file), conf);
					FileStatus[] status = fs.listStatus(new Path(file));
					Path[] listPath = FileUtil.stat2Paths(status);
					String rex = "^[correct]+.*$";
					for (Path path : listPath) {
						if (path.getName().matches(rex)) {
							System.out.println(path.toString());
							args = new String[] { "financial", "finance", "cisiondata", "192.168.0.10",
									path.toString() };
							exitCode = ToolRunner.run(new FinanceHDFS2ESJob(), args);
						}
					}

				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
