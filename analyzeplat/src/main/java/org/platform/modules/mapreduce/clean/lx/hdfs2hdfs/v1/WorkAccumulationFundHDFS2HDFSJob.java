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
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;

/**
 * 公积金
 * 
 */
public class WorkAccumulationFundHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return AccumulationFund.class;
	}

	public static void main(String[] args) {
		try {
			int exitCode = 0;
			String fss = null;
			fss = "hdfs://192.168.0.10:9000/warehouse_data/work/accumulationfund/AccumulationFund__33564f80_be04_4aac_a6f3_db25124d9b91";
			Configuration conf = new Configuration();
			FileSystem hdfs;
			hdfs = FileSystem.get(URI.create(fss), conf); // 通过uri来指定要返回的文件系统
			FileStatus[] fs = hdfs.listStatus(new Path(fss)); // FileStatus
																// 封装了hdfs文件和目录的元数据
			Path[] listPath = FileUtil.stat2Paths(fs); // 将FileStatus对象转换成一组Path对象

			for (Path p : listPath) {
				if (p.getName().contains("AccumulationFund")) {
			
					args = new String[] { "", fss, "hdfs://192.168.0.10:9000/warehouse_clean/work/accumufund/k" };
					exitCode = ToolRunner.run(new WorkAccumulationFundHDFS2HDFSJob(), args);
				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
class AccumulationFund extends BaseHDFS2HDFSV1Mapper {
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
			DataCleanUtils.fieldValidate(original, "mobilePhone", CleanUtil.phoneRex, "","idCard");
			DataCleanUtils.fieldValidate(original, "telePhone", CleanUtil.callRex, "yes","mobilePhone","idCard","cnote");
			//替换字段
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
			correct.putAll(original);
	}
}
