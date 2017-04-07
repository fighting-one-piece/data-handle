package org.platform.modules.mapreduce.clean.ljp.ES2HDFS;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseES2HDFSJob;
/**
 * 将数据导入Hadoop
 * @author Administrator
 *
 */
public class WorkHospitalES2HDFSJob2 extends BaseES2HDFSJob{

	public static void main(String[] args) {
		/**
		 * 参数1：ES节点IP
		 * 参数2：ES节点Index/Type
		 * 参数3：HDFS输出路径
		 * 参数3：HDFS输出文件记录分割条数
		 */
		try {
			long timeStar = System.currentTimeMillis();
			args = new String[]{"192.168.0.10","work/hospital","hdfs://192.168.0.10:9000/elasticsearch_original/work/hospital/10/","1500000"};
			int exitCode = ToolRunner.run(new WorkHospitalES2HDFSJob2(), args);  
			long timeEnd = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
