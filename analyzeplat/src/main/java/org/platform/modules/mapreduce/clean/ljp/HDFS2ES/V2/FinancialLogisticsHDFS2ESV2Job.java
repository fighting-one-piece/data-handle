package org.platform.modules.mapreduce.clean.ljp.HDFS2ES.V2;



import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV2Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;

/**
 * 
 * 导入规则V2,旧版本导入,同步ES数据 修改参数: 1.类型index 2.类型type 3.集群名 4.集群IP 5.输入路径
 * 6.ES批量导入大小,7每次Job的数量
 * 
 * @author Administrator
 *
 */
public class FinancialLogisticsHDFS2ESV2Job extends BaseHDFS2ESV2Job {
	public static void main(String[] args) throws Exception {
		try {
			 List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 1 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "financial", "logistics", "cisiondata", "192.168.0.10,192.168.0.11,192.168.0.12,192.168.0.13,192.168.0.14",
							"2000", sb.toString()};
					System.out.println("i>>" + i);
					System.out.println("路径:>>" + sb.toString());
					ToolRunner.run(new FinancialLogisticsHDFS2ESV2Job(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}