package org.platform.modules.mapreduce.clean.lx.hdfs2hdfs.v1;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSMapper;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;

import org.platform.modules.mapreduce.clean.util.CleanUtil;
/**
 * 母婴
 */
public class ApplicationHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return MotherAndBady.class;
	}
	
public static void main(String[] args) {		
		try {
			int exitCode = 0;
				String fss = null;
				 	fss ="hdfs://192.168.0.10:9000/warehouse_data/work/accumulationfund/";
					Configuration conf = new Configuration();
					FileSystem hdfs;
					hdfs = FileSystem.get(URI.create(fss),conf);			//通过uri来指定要返回的文件系统
					FileStatus[] fs = hdfs.listStatus(new Path(fss));		//FileStatus 封装了hdfs文件和目录的元数据
					Path[] listPath =FileUtil.stat2Paths(fs);				//将FileStatus对象转换成一组Path对象		
					int sum= 1;
					
					for(Path p : listPath){
						sum++;
						if(!p.getName().equals("_SUCCESS")&&!p.getName().equals("part-r-00000")){									
							args = new String[]{"",fss, 
									"hdfs://192.168.0.10:9000/warehouse_clean/work/accunufund/kk/"+sum};
							exitCode = ToolRunner.run(new ApplicationHDFS2HDFSJob(), args);
						}			 	
					}													
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
class MotherAndBady extends BaseHDFS2HDFSV1Mapper {
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, 
			Map<String, Object> incorrect) {
			//处理电话
			test.fieldValidate(original, "mobilePhone", CleanUtil.phoneRex, "", "idCard");
			//处理座机
			test.fieldValidate(original, "telePhone", CleanUtil.callRex, "", "idCard","mobilePhone");
			//处理身份证
			test.fieldValidate(original, "idCard", CleanUtil.idCardRex, "yes");
			
			
		correct.putAll(original);
	}
}
