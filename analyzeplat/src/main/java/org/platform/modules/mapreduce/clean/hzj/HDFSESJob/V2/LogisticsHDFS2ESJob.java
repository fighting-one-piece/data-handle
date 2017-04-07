package org.platform.modules.mapreduce.clean.hzj.HDFSESJob.V2;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.hzj.HDFSESJob.V2.LogisticsHDFS2ESJob;


public class LogisticsHDFS2ESJob  extends BaseHDFS2ESV2Job{
	public static void main(String[] args) throws Exception {
//		try {
//
//			String n;
//			for (int j = 1; j <= 3; j++) {
//				for (int i = 0; i <= 19; i++) {
//					if (i < 10) {
//						n = "0" + i;
//					} else {
//						n = "" + i;
//					}
//
//					String file = "hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial/logistics/21/records-"+j 
//							+ "-m-000" + n;
//					Configuration conf = new Configuration();
//					FileSystem fs = FileSystem.get(URI.create(file), conf);
//					FileStatus[] status = fs.listStatus(new Path(file));
//					Path[] listPath = FileUtil.stat2Paths(status);
//					String rex = "^[correct]+.*$";
//					for (Path path : listPath) {
//						if (path.getName().matches(rex)) {
//							System.out.println(path.toString());
//							args = new String[] { "financial", "logistics", "cisiondata","192.168.0.10,192.168.0.12",
//									path.toString() };
//							exitCode = ToolRunner.run(new LogisticsHDFS2ESJob(), args);
//						}
//					}
//
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		args = new String[] { "financial", "logistics", "cisiondata","192.168.0.114",
				"2000","hdfs://192.168.0.115:9000/user/ljpTest/logistics/correct-m-00011" };
		ToolRunner.run(new LogisticsHDFS2ESJob(), args);
			System.exit(0);
	}
}
