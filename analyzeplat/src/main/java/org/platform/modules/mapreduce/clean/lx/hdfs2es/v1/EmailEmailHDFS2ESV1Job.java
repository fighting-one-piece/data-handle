package org.platform.modules.mapreduce.clean.lx.hdfs2es.v1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
/**
 * V1版本需要设置输入输出参数
 * @author lixin
 */
public class EmailEmailHDFS2ESV1Job extends BaseHDFS2ESV1Job{
	public static void main(String[] args) {
		try {
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0], "^(correct)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 5 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] args1 = new String[] { "", "", "cisiondata", "192.168.0.10,192.168.0.12",
							sb.toString(),args[1]+i};
					ToolRunner.run(new EmailEmailHDFS2ESV1Job(), args1);
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
