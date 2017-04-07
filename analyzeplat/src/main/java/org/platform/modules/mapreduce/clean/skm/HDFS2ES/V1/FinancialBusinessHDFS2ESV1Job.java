package org.platform.modules.mapreduce.clean.skm.HDFS2ES.V1;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
/**
 * 清洗后数据导入ES
 * 
 * @author Administrator
 *
 */
public class FinancialBusinessHDFS2ESV1Job extends BaseHDFS2ESV1Job {
	public static void main(String[] args) {
		try {	
//			args = new String[]{"hdfs://192.168.0.10:9000/warehouse_clean/financial/business"};
			List<String> filePathList = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[0],"^(correct)+.*$", filePathList);	
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = filePathList.size(); i < len; i++) {
				sb.append(filePathList.get(i)).append(",");
				if ((i % 5 == 0 || i == (len - 1))&&i!=0) {
					sb.deleteCharAt(sb.length() - 1);
				//	System.out.println(sb.toString());
					String[]  args1= new String[] { "financial_new", "business", "cisiondata", 
							"192.168.0.10,192.168.0.11", "1000",sb.toString(),args[1]+i};
					ToolRunner.run(new FinancialBusinessHDFS2ESV1Job(),args1);
					sb = new StringBuilder();
					
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
