package org.platform.modules.mapreduce.clean.lx.hdfs2hdfs.v1;

import java.net.URI;
import java.util.HashMap;
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
/**
 * emailClean
 */
public class AccumulationFundHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return EmailmailV2.class;
	}
	
public static void main(String[] args) {		
		try {
			int exitCode = 0;
//			for(int j = 1;j <=14;j++){
				String fss = null;
//				for (int i = 0; i <=9; i++) {
//					if(i>=10){
//						 fss ="hdfs://192.168.0.10:9000/elasticsearch_original/email/mailbox/11/records-"+j+"-m-000"+i;
//					}else {
//						 fss ="hdfs://192.168.0.10:9000/elasticsearch_original/email/mailbox/11/records-"+j+"-m-0000"+i;
//					}	
				 	fss = "hdfs://192.168.0.10:9000/warehouse_data/work/accumulationfund/AccumulationFund__f8bf1c5d_b2e4_49b0_9c29_e1b60a74bf34";
					Configuration conf = new Configuration();
					FileSystem hdfs;
					hdfs = FileSystem.get(URI.create(fss),conf);			//通过uri来指定要返回的文件系统
					FileStatus[] fs = hdfs.listStatus(new Path(fss));		//FileStatus 封装了hdfs文件和目录的元数据
					Path[] listPath =FileUtil.stat2Paths(fs);				//将FileStatus对象转换成一组Path对象		
					for(Path p : listPath){
						if(!p.getName().equals("_SUCCESS")&&!p.getName().equals("AccumulationFund")){									
							args = new String[]{"",fss, 
									"hdfs://192.168.0.10:9000/warehouse_clean/work/test/S"};
							exitCode = ToolRunner.run(new AccumulationFundHDFS2HDFSJob(), args);
						}			 	
					}													
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
class EmailmailV2 extends BaseHDFS2HDFSV1Mapper {
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, 
			Map<String, Object> incorrect) {
		System.out.println(original.toString());
		Map<String, Object> erro = new HashMap<String, Object>();		
		erro.putAll(original);	//原始的错误数据		
		//处理空格

		
		
		//清洗字段
		if(original.containsKey("percentOfOrganiza")){
			original.put("paymentAmount", original.get("percentOfOrganiza"));
			original.remove("percentOfOrganiza");
		}
		if(original.containsKey("costEndTime")){
			original.put("closingTime", original.get("costEndTime"));
			original.remove("costEndTime");
		}
		if(original.containsKey("openDate")){
			original.put("openingTime", original.get("openDate"));
			original.remove("openDate");
		}
		
		
		
	}
}
