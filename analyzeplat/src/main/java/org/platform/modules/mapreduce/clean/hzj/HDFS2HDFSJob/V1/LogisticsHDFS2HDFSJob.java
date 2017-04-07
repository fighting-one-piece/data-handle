package org.platform.modules.mapreduce.clean.hzj.HDFS2HDFSJob.V1;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.hzj.Tool.LogisticsTool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class LogisticsHDFS2HDFSJob extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		// TODO Auto-generated method stub
		return FinancialLogisticsHDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;
			String n;
			for (int j = 1; j <= 16; j++) {
				for (int i = 0; i <= 9; i++) {
					if (i < 10) {
						n = "0" + i;
					} else {
						n = "" + i;
					}

					args = new String[] {
							"hdfs://192.168.0.10:9000/elasticsearch_original/financial/logistics/11/records-"+j
									+ "-m-000"+n ,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/financial/logistics/11/records-"+j
									+ "-m-000"+n };
					exitCode = ToolRunner.run(new LogisticsHDFS2HDFSJob(), args);
		       }
			}

			long timeEnd = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class FinancialLogisticsHDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> incorrectMap = new HashMap<String, Object>();
		incorrectMap.putAll(original);
		// 替换空格
		Map<String, Object> map = CleanUtil.replaceSpace(original);
		// 有效字段的个数
		boolean flag = false;
		int times = 0;
		Iterator<Map.Entry<String, Object>> maps = map.entrySet().iterator();
		while (maps.hasNext()) {
			Map.Entry<String, Object> entry = maps.next();
			if (!"NA".equals(entry.getValue()) && !"null".equals(entry.getValue()) && !"".equals(entry.getValue())) {
				times++;
			}
		}
		// 满足正则表达式的次数
		int sum = 0;
		if (original.containsKey("idCard")) {
			String idCard = (String) map.get("idCard");
			if (CleanUtil.matchIdCard(idCard)) {
				List<String> list = LogisticsTool.findOther("idCard", idCard);
				if (list != null) {
					map = LogisticsTool.addCnote(map, "idCard", list.get(1));
					map.put("idCard", list.get(0));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "idCard");
				if (list.size() > 0) {
					map.put("idCard", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("idCard", "NA");
				}
			}
		}

		if (original.containsKey("linkIdCard")) {
			String linkIdCard = (String) map.get("linkIdCard");
			if (CleanUtil.matchIdCard(linkIdCard)) {
				List<String> list = LogisticsTool.findOther("linkIdCard", linkIdCard);
				if (list != null) {
					map.put("linkIdCard", list.get(0));
					map = LogisticsTool.addCnote(map, "linkIdCard", list.get(1));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "linkIdCard");
				if (list.size() > 0) {
					map.put("linkIdCard", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				}
			}
		}

		if (original.containsKey("phone")) {
			String phone = (String) map.get("phone");
			if (CleanUtil.matchPhone(phone) || CleanUtil.matchCall(phone.replaceAll("[(]|[)]", "-"))) {
				List<String> list = LogisticsTool.findOther("phone", phone);
				if (list != null) {
					map = LogisticsTool.addCnote(map, "phone", list.get(1));
					map.put("phone", list.get(0));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "phone");
				if (list.size() > 0) {
					map.put("phone", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("phone", "NA");
				}
			}
		}
		if (original.containsKey("rcall")) {
		   String rcall = (String) map.get("rcall");
			if(CleanUtil.isAllHalf(rcall)){
				rcall = CleanUtil.ToDBC(rcall);
			}
			if (CleanUtil.matchPhone(rcall) || CleanUtil.matchCall(rcall.replaceAll("[(]|[)]", "-"))) {
				List<String> list = LogisticsTool.findOther("rcall", rcall);
				if (list != null) {
					map.put("rcall", list.get(0));
					map = LogisticsTool.addCnote(map, "rcall", list.get(1));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "rcall");
				if (list.size() > 0) {
					map.put("rcall", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("rcall", "NA");
				}
			}
		}

		if (original.containsKey("linkPhone")) {
			String linkPhone = (String) map.get("linkPhone");
			if (CleanUtil.matchPhone(linkPhone) || CleanUtil.matchCall(linkPhone.replaceAll("[(]|[)]", "-"))) {
				List<String> list = LogisticsTool.findOther("linkPhone", linkPhone);
				if (list != null) {
					map.put("linkPhone", list.get(0));
					map = LogisticsTool.addCnote(map, "linkPhone", list.get(1));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "linkPhone");
				if (list.size() > 0) {
					map.put("linkPhone", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("linkPhone", "NA");
				}
			}
		}
		if (original.containsKey("linkCall")) {
			String linkCall = (String) map.get("linkCall");
			if(CleanUtil.isAllHalf(linkCall)){
				linkCall = CleanUtil.ToDBC(linkCall);
			}
			if (CleanUtil.matchCall(linkCall) || CleanUtil.matchCall(linkCall.replaceAll("[(]|[)]", "-"))) {
				List<String> list = LogisticsTool.findOther("linkCall", linkCall);
				if (list != null) {
					map.put("linkCall", list.get(0));
					map = LogisticsTool.addCnote(map, "linkCall", list.get(1));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "linkCall");
				if (list.size() > 0) {
					map.put("linkCall", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("linkCall", "NA");
				}
			}
		}
		if (original.containsKey("email")) {
			String email = (String) map.get("email");
			if (CleanUtil.matchEmail(email)) {
				List<String> list = LogisticsTool.findOther("email", email);
				if (list != null) {
					map.put("email", list.get(0));
					map = LogisticsTool.addCnote(map, "email", list.get(1));
					sum++;
				}
			} else {
				List<String> list = LogisticsTool.findMatch(map, "email");
				if (list.size() > 0) {
					map.put("email", list.get(0));
					map.put(list.get(1), list.get(2));
					sum++;
				} else {
					map.put("email", "NA");
				}
			}
		}

		if (!flag) {
			if (original.containsKey("name") && original.containsKey("address")
					&& CleanUtil.matchChinese((String) original.get("name"))
					&& CleanUtil.matchChinese((String) original.get("address"))) {
				flag = true;
			} else if ((original.containsKey("expressId")
					&& CleanUtil.matchNumAndLetter((String) original.get("expressId")))
					&& (original.containsKey("name") && CleanUtil.matchChinese((String) original.get("name")))) {
                
				flag = true;
			} else if ((original.containsKey("orderId")
					&& CleanUtil.matchNumAndLetter((String) original.get("orderId")))
					&& (original.containsKey("name") && CleanUtil.matchChinese((String) original.get("name")))) {
				flag = true;
			} else if (original.containsKey("linkName") && original.containsKey("linkAddress")
					&& CleanUtil.matchChinese((String) original.get("linkName"))
					&& CleanUtil.matchChinese((String) original.get("linkAddress"))) {
				flag = true;
			}
		}

		if ((sum > 0 || flag) && times > 5) {
			correct.putAll(changeKey(map));
		} else {
			incorrect.putAll(incorrectMap);
		}
	}

	public Map<String, Object> changeKey(Map<String, Object> map) {
		if (map.containsKey("linkPhone")) {
			Object value = map.get("linkPhone");
			map.remove("linkPhone");
			map.put("linkMobilePhone", value);
		}
		if (map.containsKey("linkCall")) {
			Object value = map.get("linkCall");
			map.remove("linkCall");
			map.put("linkTelePhone", value);
		}
		if (map.containsKey("phone")) {
			Object value = map.get("phone");
			map.remove("phone");
			map.put("mobilePhone", value);
		}
		if (map.containsKey("rcall")) {
			Object value = map.get("rcall");
			map.remove("rcall");
			map.put("telePhone", value);
		}
		if (map.containsKey("bankCard")) {
			Object value = map.get("bankCard");
			map.remove("bankCard");
			map.put("cardNo", value);
		}
		/** 10集群中快递公司名称为 expressName 不可修改*/
		/*if (map.containsKey("expressName")) {
			Object value = map.get("expressName");
			map.remove("expressName");
			map.put("expressCompanyName", value);
		}*/
		if (map.containsKey("name")) {
			Object value = map.get("name");
			map.put("nameAlias", value);
		}
		if (map.containsKey("linkName")) {
			Object value = map.get("linkName");
			map.put("linkNameAlias", value);
		}
		return map;
	}

}