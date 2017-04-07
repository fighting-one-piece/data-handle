package org.platform.modules.mapreduce.clean.skm.HDFS2ES.V1;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2ESV1Job;

public class TestBusinessHDFS2ESJob extends BaseHDFS2ESV1Job{

	public static void main(String[] args) {
		long starTime = System.currentTimeMillis();
		int exitCode = 0;	
		String fss ="hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial_new/business/11/records-2-m-00004/correct-m-00002";
				Configuration conf = new Configuration();
				FileSystem hdfs;
				String rex = "^correct.*$";
				try {
					hdfs = FileSystem.get(URI.create(fss), conf); //通过uri来指定要返回的文件系统
					FileStatus[] fs = hdfs.listStatus(new Path(fss)); //FileStatus 封装了hdfs文件和目录的元数据
					Path[] listPath = FileUtil.stat2Paths(fs); //将FileStatus对象转换成一组Path对象
					for (Path p:listPath){
						if (p.getName().matches(rex)){
							
							System.out.println(p.toString());
							args = new String[]{"financial_new","business","cisiondata","192.168.0.10,192.168.0.12",p.toString()};
							exitCode = ToolRunner.run(new TestBusinessHDFS2ESJob(), args);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			
		
		long endTime = System.currentTimeMillis();
		SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
		Date date = new Date(endTime - starTime);
		System.out.println("用时:" + formatter.format(date));
		System.exit(exitCode); 
//		try {
//			args = new String[]{"work","socialsecurity","cisiondata","192.168.0.10",
//			"hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/socialsecurity/21/records-1-m-00019/correct-m-00000"};
//			int exitCode;
//			exitCode = ToolRunner.run(new BaseHDFS2ESJob(), args);
//			System.exit(exitCode);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}
}
