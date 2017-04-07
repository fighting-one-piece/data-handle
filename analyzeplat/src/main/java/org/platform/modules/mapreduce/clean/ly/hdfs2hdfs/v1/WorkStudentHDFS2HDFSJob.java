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
import org.platform.modules.mapreduce.clean.ly.cleanTool.StudentTool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class WorkStudentHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return WorkStudentHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		//fss:文件输入路径       "hdfs://192.168.0.115:9000/elasticsearch/work/20"
		if(args.length!=3){
			System.out.println("参数不正确");
			System.exit(2);
		}
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
					//"hdfs://192.168.0.115:9000/elasticsearch_clean/work/student/20"
					args1 = new String[] {args[0],
							p.toString(),
							args[2]+"/" + p.getName() };
					System.out.println("执行文件--------------->>>>>>>>>>>>>"+p.getName());
					exitCode = ToolRunner.run(new WorkStudentHDFS2HDFSJob(), args1);
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
	class WorkStudentHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

		@Override
		public void handle(Map<String, Object> original,
				Map<String, Object> correct, Map<String, Object> incorrect) {
			int sum = 0;
			Map<String, Object> map = CleanUtil.replaceSpace(original);
			// 有效字段的个数
			int times = 0;
			//标示性字段的开关
			boolean flag = false;
			if (original.containsKey("idCard")) {
				String idCard = (String) map.get("idCard");
				// 身份证号码判断
				if (CleanUtil.matchIdCard(idCard)) {
					List<String> list = StudentTool.findOther("idCard", idCard);
					if (list != null) {
						map = StudentTool
								.addCnote(map,list.get(1));
						map.put("idCard", list.get(0));
						sum++;
					}
				} else {
					List<String> list = StudentTool.findMatch(map, "idCard");
					if (list.size() > 0) {
						map.put("idCard", list.get(0));
						map.put(list.get(1), list.get(2));
						sum++;
					} else {
						map.put("idCard", "NA");
					}
				}
			}
				// 监护人身份证号码判断
				if (original.containsKey("lgIdCard")) {
					String lgIdCard = (String) map.get("lgIdCard");
					if (CleanUtil.matchIdCard(lgIdCard)) {
						List<String> list = StudentTool.findOther("lgIdCard",
								lgIdCard);
						if (list != null) {
							map = StudentTool.addCnote(map,list.get(1));
							map.put("lgIdCard", list.get(0));
							sum++;
						}
					} else {
						List<String> list = StudentTool.findMatch(map,
								"lgIdCard");
						if (list.size() > 0) {
							map.put("lgIdCard", list.get(0));
							map.put(list.get(1), list.get(2));
							sum++;
						} else {
							map.put("lgIdCard", "NA");
						}
					}
				}
				// 手机判断
				if (original.containsKey("phone")) {
					String phone = (String) map.get("phone");
					if (CleanUtil.matchPhone(phone)
							|| CleanUtil.matchCall(phone)) {
						List<String> list = StudentTool.findOther("phone",
								phone);
						if (list != null) {
							map = StudentTool.addCnote(map,list.get(1));
							map.put("phone", list.get(0));
							sum++;
						}
					} else {
						List<String> list = StudentTool.findMatch(map, "phone");
						if (list.size() > 0) {
							map.put("phone", list.get(0));
							map.put(list.get(1), list.get(2));
							sum++;
						} else {
							map.put("phone", "NA");
						}
					}
				}

				// 监护人电话判断
				if (original.containsKey("lgphone")) {
					String lgphone = (String) map.get("lgphone");
					if (CleanUtil.matchPhone(lgphone)
							|| CleanUtil.matchCall(lgphone)) {
						List<String> list = StudentTool.findOther("lgphone",
								lgphone);
						if (list != null) {
							map = StudentTool.addCnote(map, list.get(1));
							map.put("lgphone", list.get(0));
							sum++;
						}
					} else {
						List<String> list = StudentTool.findMatch(map,
								"lgphone");
						if (list.size() > 0) {
							map.put("lgphone", list.get(0));
							map.put(list.get(1), list.get(2));
							sum++;
						} else {
							map.put("lgphone", "NA");
						}
					}
				}

				// 家庭电话判断
				if (original.containsKey("call")) {
					String call = (String) map.get("call");
					if (CleanUtil.matchCall(call) || CleanUtil.matchPhone(call)) {
						List<String> list = StudentTool.findOther("call", call);
						if (list != null) {
							map.put("call", list.get(0));
							map = StudentTool.addCnote(map,list.get(1));
							sum++;
						}
					} else {
						List<String> list = StudentTool.findMatch(map, "call");
						if (list.size() > 0) {
							map.put("call", list.get(0));
							map.put(list.get(1), list.get(2));
							sum++;
						} else {
							map.put("call", "NA");
						}
					}
				}
				if(original.containsKey("qqNum")){
					String qq = map.get("qqNum").toString();
					if(CleanUtil.matchQQ(qq)){
						sum++;
					}
				}
				if(original.containsKey("email")){
					String email = map.get("email").toString();
					if(CleanUtil.matchEmail(email)){
						sum++;
					}
				}
				//当没有主要字段或没有满足条件  即：sum=0,且需要看是否存在标示性的字段
				if(sum == 0){
					flag = StudentTool.togetherMatch(map);
					if(!flag){
						if(map.containsKey("formalities") && CleanUtil.matchNumAndLetter((String)map.get("formalities"))){
							flag = true;
						}else if(map.containsKey("studentCode") && CleanUtil.matchNumAndLetter((String)map.get("studentCode"))){
							flag = true;
						}
					}
				}
				
				
				Iterator<Map.Entry<String, Object>> maps = map.entrySet()
						.iterator();
				while (maps.hasNext()) {
					Map.Entry<String, Object> entry = maps.next();
					if(!"NA".equals(entry.getValue()) && !"null".equals(entry.getValue()) && !"".equals(entry.getValue())) {
						times++;
					}
				}
				
				// 放入相应的集合
				if ((sum>0 || flag)&& times > 5) {
					correct.putAll(StudentTool.replaceKey(map));
				} else {
					incorrect.putAll(original);
				}

			}
		}