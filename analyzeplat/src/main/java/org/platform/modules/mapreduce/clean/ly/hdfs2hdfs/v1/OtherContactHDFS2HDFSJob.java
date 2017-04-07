package org.platform.modules.mapreduce.clean.ly.hdfs2hdfs.v1;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.platform.modules.mapreduce.clean.ly.cleanTool.ContactTool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

/**
 * 通讯录清洗
 * @author Administrator
 *
 */
public class OtherContactHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return OtherContactHDFS2HDFSMapper.class;
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		if(args.length!=3){
			System.out.println("参数不正确");
			System.exit(2);
		}
//		args = new String[]{"hdfs://192.168.0.10:9000/elasticsearch_original/other/contact/21","hdfs://192.168.0.10:9000/elasticsearch_clean_1/other/contact/21"};
		String fss = args[1];
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
					args1 = new String[] {args[0],
							p.toString(),
							args[2]+"/" + p.getName() };
					System.out.println("执行文件--------------->>>>>>>>>>>>>"+p.getName());
					exitCode = ToolRunner.run(new OtherContactHDFS2HDFSJob(), args1);
				}
			}
			long endTime = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(endTime - startTime);
			System.out.println("用时--------->>>" +formatter.format(date));
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

	class OtherContactHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

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
					List<String> list = ContactTool.findOther("idCard", idCard);
					if (list != null) {
						map = ContactTool.addCnote(map, list.get(1));
						map.put("idCard", list.get(0));
						sum++;
					}
				} else {
					List<String> list = ContactTool.findMatch(map, "idCard");
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
						List<String> list = ContactTool.findOther("phone", phone);
						if (list != null) {
							map = ContactTool.addCnote(map,list.get(1));
							map.put("phone", list.get(0));
							sum++;
						}
					} else {
						List<String> list = ContactTool.findMatch(map, "phone");
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
				if (original.containsKey("homeCall")) {
					String homeCall = (String) map.get("homeCall");
					if (CleanUtil.matchCall(homeCall) || CleanUtil.matchPhone(homeCall)) {
						List<String> list = ContactTool.findOther("homeCall", homeCall);
						if (list != null) {
							map.put("homeCall", list.get(0));
							map = ContactTool.addCnote(map, list.get(1));
							sum++;
						}
					} else {
						List<String> list = ContactTool.findMatch(map, "homeCall");
						if (list.size() > 0) {
							map.put("homeCall", list.get(0));
							map.put(list.get(1), list.get(2));
							sum++;
						} else {
							map.put("homeCall", "NA");
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
				if(original.containsKey("msn")){
					String qq = map.get("msn").toString();
					if(CleanUtil.matchNumAndLetter(qq)){
						sum++;
					}
				}
				//当没有主要字段或没有满足条件  即：sum=0,且需要看是否存在标示性的字段
				//标示性字段的开关
				boolean flag = false;
				if(sum==0){
					flag = ContactTool.togetherMatch(map);
				}
				
				Iterator<Map.Entry<String, Object>> maps = map.entrySet().iterator();
				while (maps.hasNext()) {
					Map.Entry<String, Object> entry = maps.next();
					if (!"NA".equals(entry.getValue()) && !"null".equals(entry.getValue()) && !"".equals(entry.getValue())) {
						times++;
					}
				}
				// 放入相应的集合
				if ((sum > 0 || flag)&& times > 5) {
					correct.putAll(ContactTool.replaceKey(map));
				} else {
					incorrect.putAll(original);
				}
			}
		}