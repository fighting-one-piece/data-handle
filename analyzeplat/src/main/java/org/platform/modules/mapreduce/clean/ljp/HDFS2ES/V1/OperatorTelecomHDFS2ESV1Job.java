package org.platform.modules.mapreduce.clean.ljp.HDFS2ES.V1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

/**
 * 
 * 导入规则V2,新版本导入,同步ES数据 修改参数: 1.类型index 2.类型type 3.集群名 4.集群IP 5.输入路径 6.匹配文件正则表达式
 * 7.输出HDFS路径
 * 
 * @author Administrator
 *
 */
public class OperatorTelecomHDFS2ESV1Job extends BaseHDFS2ESV1Job {
	public static void main(String[] args) {
		try {
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 5 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "operator", "telecom", "cisiondata", "192.168.0.10,192.168.0.12", sb.toString(),
							args[1]+i };
					System.out.println("i>>"+i);
					System.out.println("路径:>>"+sb.toString());
					ToolRunner.run(new OperatorTelecomHDFS2ESV1Job(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
