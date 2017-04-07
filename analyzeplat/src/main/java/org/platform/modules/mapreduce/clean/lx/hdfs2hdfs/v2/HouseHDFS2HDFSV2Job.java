package org.platform.modules.mapreduce.clean.lx.hdfs2hdfs.v2;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;


/**
 * housClean
 */
public class HouseHDFS2HDFSV2Job extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		
		return Financial_newHouse.class;
	}	
	
public static void main(String[] args) {
		try {											
			int exitCode = 0;	
//
				for(int i=0; i<9;i++){
					String fss ="hdfs://192.168.0.10:9000/elasticsearch_original/financial_new/house/105/records-1-m-0000"+i;
					Configuration conf = new Configuration();
					FileSystem hdfs;
					hdfs = FileSystem.get(URI.create(fss),conf);	//通过uri来指定要返回的文件系统
					FileStatus[] fs = hdfs.listStatus(new Path(fss));	//FileStatus 封装了hdfs文件和目录的元数据
					Path[] listPath =FileUtil.stat2Paths(fs);	//将FileStatus对象转换成一组Path对象		
					for(Path p : listPath){
						if(!p.getName().equals("_SUCCESS")&&!p.getName().equals("part-r-00000")){									
							args = new String[]{fss, 
									"hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial_new/house/105/"+p.getName()};	
							exitCode = ToolRunner.run(new HouseHDFS2HDFSV2Job(), args);
						}			 	
					}												
				}						
			System.exit(exitCode);			       
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}

class Financial_newHouse extends BaseHDFS2HDFSV1Mapper {
	
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, 
			Map<String, Object> incorrect) {
		
	}
}

