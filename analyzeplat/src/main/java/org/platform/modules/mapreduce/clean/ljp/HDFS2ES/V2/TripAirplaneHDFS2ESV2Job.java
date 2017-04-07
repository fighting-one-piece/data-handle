package org.platform.modules.mapreduce.clean.ljp.HDFS2ES.V2;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;

/**
 * 
 * 导入规则V2,旧版本导入,同步ES数据 修改参数: 1.类型index 2.类型type 3.集群名 4.集群IP 5.输入路径 6.ES批量导入大小
 * 
 * @author Administrator
 *
 */
public class TripAirplaneHDFS2ESV2Job extends BaseHDFS2ESV2Job{
	public static void main(String[] args) {
		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;
			String file = null;
			String rex = "^[correct]+.*$";
			int index = 0;
			for(index=1;index<=1;index++){
			for (int i = 0; i <= 19; i++) {
				if(i < 10){
				 file = "hdfs://192.168.0.10:9000/elasticsearch_clean_1/trip/airplane/21/records-"+index+"-m-0000" + i
						+ "/";
				}else{
			     file = "hdfs://192.168.0.10:9000/elasticsearch_clean_1/trip/airplane/21/records-"+index+"-m-000" + i
						+ "/";
				}
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(URI.create(file), conf);
				FileStatus[] status = fs.listStatus(new Path(file));
				Path[] listPath = FileUtil.stat2Paths(status);
				for (Path path : listPath) {
					if (path.getName().matches(rex)) {
						args = new String[] { "trip", "airplane", "cisiondata", "192.168.0.10,192.168.0.12", path.toString() };
						exitCode = ToolRunner.run(new TripAirplaneHDFS2ESV2Job(), args);
				}
			}
		}
	}
			long timeEnd = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
			/*
			 * args = new
			 * String[]{"ljp_trip","airplane","youmeng","192.168.0.114",
			 * "hdfs://192.168.0.115:9000/elasticsearch/trip/airplane_out/correct-m-00000"
			 * }; exitCode = ToolRunner.run(new BaseHDFS2ESJob(), args);
			 */
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
