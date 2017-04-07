package org.platform.modules.mapreduce.clean.tjl;

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

public class CybercafeHDFS2ESJob extends BaseHDFS2ESV2Job {

	public static void main(String[] args) {
		long starTime = System.currentTimeMillis();
		int exitCode = 0;
		String n;
		for (int j=4; j<=4; j++){
			for (int i=0; i<=9; i++){
				if (i<10){
					n = "0"+i;
				} else {
					n = "" + i;
				}
			String fss ="hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/cybercafe/20/records-"+j+"-m-000"+n;//s要小写
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
							args = new String[]{"work","cybercafe","cisiondata","192.168.0.10",p.toString()};
							exitCode = ToolRunner.run(new CybercafeHDFS2ESJob(), args);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		long endTime = System.currentTimeMillis();
		SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
		Date date = new Date(endTime - starTime);
		System.out.println("用时:" + formatter.format(date));
		System.exit(exitCode); 
	}
}
