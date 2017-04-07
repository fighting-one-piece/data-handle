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
public class OperatorTelecomES2HDFSJob extends BaseES2HDFSJob{

	public static void main(String[] args) {
		/**
		 * 参数1：ES节点IP
		 * 参数2：ES节点Index/Type
		 * 参数3：HDFS输出路径
		 * 参数3：HDFS输出文件记录分割条数
		 */
		try {
			long timeStar = System.currentTimeMillis();
			args = new String[]{"192.168.0.114","email/mailbox","hdfs://192.168.0.115:9000/user/ljpTest/email","1000000"};
			int exitCode = ToolRunner.run(new OperatorTelecomES2HDFSJob(), args);
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
