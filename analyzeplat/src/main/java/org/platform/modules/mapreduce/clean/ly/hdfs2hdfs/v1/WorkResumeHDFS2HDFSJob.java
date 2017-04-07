package org.platform.modules.mapreduce.clean.ly.hdfs2hdfs.v1;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.ly.cleanTool.ResumeTool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

/**
 * 简历清理逻辑
 * 
 * @author Administrator
 *
 */
public class WorkResumeHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return WorkResumeHDFS2HDFSMapper.class;
	}
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		if(args.length!=3){
			System.out.println("参数不正确");
			System.exit(2);
		}
//		args = new String[]{"hdfs://192.168.0.10:9000/elasticsearch_original/work/resume/20","hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/resume/20"};
		//hdfs:192.168.0.10:9000/elasticsearch/work/resume/21
		String fss =args[1];
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			String[] args1;
			hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			int exitCode = 0;
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS")
						&& !p.getName().equals("part-r-00000")) {
					//三个参数
					args1 = new String[] {args[0],
							p.toString(),
							args[2]+"/" + p.getName() };
					System.out.println("执行文件--------------->>>>>>>>>>>>>"+p.getName());
					exitCode = ToolRunner.run(new WorkResumeHDFS2HDFSJob(), args1);
				}
			}
			long endTime = System.currentTimeMillis();
			System.out.println("用时--------->>>" + ((endTime - startTime) / 1000) + "s");
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
}
}
class WorkResumeHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original,
			Map<String, Object> correct, Map<String, Object> incorrect) {
		int sum = 0;
		Map<String, Object> map = CleanUtil.replaceSpace(original);
		// 有效字段的个数
		int times = 0;
		if (original.containsKey("idCard")) {
			String idCard = (String) map.get("idCard");
			// 身份证号码判断
			if (CleanUtil.matchIdCard(idCard)) {
				List<String> list = ResumeTool.findOther("idCard", idCard);
				if (list != null) {
					map = ResumeTool.addCnote(map, list.get(1));
					map.put("idCard", list.get(0));
					sum++;
				}
			} else {
				List<String> list = ResumeTool.findMatch(map, "idCard");
				if (list.size() > 0) {
					map.put("idCard", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("idCard", "NA");
				}
			}
		}
			// 手机判断
			if (original.containsKey("phone")) {
				String phone = (String) map.get("phone");
				if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone)) {
					List<String> list = ResumeTool.findOther("phone", phone);
					if (list != null) {
						map = ResumeTool.addCnote(map,list.get(1));
						map.put("phone", list.get(0));
						sum++;
					}
				} else {
					List<String> list = ResumeTool.findMatch(map, "phone");
					if (list.size() > 0) {
						map.put("phone", list.get(0));
						map.put(list.get(1), list.get(2));
						sum++;
					} else {
						map.put("phone", "NA");
					}
				}
			}
			// 家庭电话判断
			if (original.containsKey("homeCallNo")) {
				String homeCallNo = (String) map.get("homeCallNo");
				if (CleanUtil.matchCall(homeCallNo) || CleanUtil.matchPhone(homeCallNo)) {
					List<String> list = ResumeTool.findOther("homeCallNo", homeCallNo);
					if (list != null) {
						map.put("homeCallNo", list.get(0));
						map = ResumeTool.addCnote(map,list.get(1));
						sum++;
					}
				} else {
					List<String> list = ResumeTool.findMatch(map, "homeCallNo");
					if (list.size() > 0) {
						map.put("homeCallNo", list.get(0));
						map.put(list.get(1), list.get(2));
						sum++;
					} else {
						map.put("homeCallNo", "NA");
					}
				}
			}
			//工作电话
			if (original.containsKey("workCallNo")) {
				String workCallNo = (String) map.get("workCallNo");
				if (CleanUtil.matchCall(workCallNo) || CleanUtil.matchPhone(workCallNo)) {
					List<String> list = ResumeTool.findOther("workCallNo", workCallNo);
					if (list != null) {
						map.put("workCallNo", list.get(0));
						map = ResumeTool.addCnote(map, list.get(1));
						sum++;
					}
				} else {
					List<String> list = ResumeTool.findMatch(map, "workCallNo");
					if (list.size() > 0) {
						map.put("workCallNo", list.get(0));
						map.put(list.get(1), list.get(2));
						sum++;
					} else {
						map.put("workCallNo", "NA");
					}
				}
			}
			if(original.containsKey("email")){
				String email = map.get("email").toString();
				if(CleanUtil.matchEmail(email)){
					sum++;
				}
			}
			if(original.containsKey("qq")){
				String qq = map.get("qq").toString();
				if(CleanUtil.matchQQ(qq)){
					sum++;
				}
			}
			//当没有主要字段或没有满足条件  即：sum=0,且需要看是否存在标示性的字段
			//标示性字段的开关
			boolean flag = false;
			if(sum==0){
				flag = ResumeTool.togetherMatch(map);
			}
			
			//有效字段个数判断
			Iterator<Map.Entry<String, Object>> maps = map.entrySet().iterator();
			while (maps.hasNext()) {
				Map.Entry<String, Object> entry = maps.next();
				if (!"NA".equals(entry.getValue()) && !"null".equals(entry.getValue()) && !"".equals(entry.getValue())) {
					times++;
				}
			}
			// 放入相应的集合
			if ((sum>0 || flag)&& times > 5) {
				correct.putAll(ResumeTool.replaceKey(map));
			} else {
				incorrect.putAll(original);
			}
		}
	}