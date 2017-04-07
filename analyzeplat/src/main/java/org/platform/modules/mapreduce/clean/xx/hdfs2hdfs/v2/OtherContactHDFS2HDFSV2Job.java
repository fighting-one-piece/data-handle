package org.platform.modules.mapreduce.clean.xx.hdfs2hdfs.v2;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV2Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.xx.util.CleanTool;
import org.platform.modules.mapreduce.clean.xx.util.ReadFirstLineUtil;

public class OtherContactHDFS2HDFSV2Job extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV2Mapper> getMapperClass() {
		return OtherContactHDFS2HDFSV2Mapper.class;
	}

	public static void main(String[] args) {
		try {
			String fss = "hdfs://192.168.0.115:9000/user/xx/other/";
			Configuration conf = new Configuration();
			FileSystem hdfs;
			hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS") && !p.getName().equals("part-r-00000")) {
					// 读取文件第一行
					String line = ReadFirstLineUtil.readFirstLine(hdfs, p);
					if (line != null) {
						String[] args1 = new String[] { line, p.toString(),
								"hdfs://192.168.0.115:9000/user/xx/test/" + p.getName() };
						System.out.println("执行文件：" + p.getName());
						ToolRunner.run(new OtherContactHDFS2HDFSV2Job(), args1);
					}
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class OtherContactHDFS2HDFSV2Mapper extends BaseHDFS2HDFSV2Mapper {
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> erro = new HashMap<String, Object>();
		erro.putAll(original);
		// 处理空格
		original = CleanTool.replaceSpace(original);
		String cardReg = CleanUtil.idCardRex;
		String phoneReg = CleanUtil.phoneRex;
		String callReg = CleanUtil.callRex;
		Pattern patternCard = Pattern.compile(CleanUtil.idCardRex);
		Pattern patternPhone = Pattern.compile(CleanUtil.phoneRex);
		Pattern patternCall = Pattern.compile(CleanUtil.callRex);
		boolean flag = true;
		// **********************************************idCard******************************************
		if (original.containsKey("cardName")) {
			String cardType = (String) original.get("cardName");
			if (cardType.equals("居民身份证")) {
				if (original.containsKey("idCard")) {
					String idCard = (String) original.get("idCard");
					if (CleanUtil.matchIdCard(idCard)) {
						CleanTool.clean(original, "idCard", idCard, patternCard, cardReg);
					} else {
						flag = CleanTool.cleanT(original, "idCard", patternCard, cardReg, "Contact");
					}
				} else {
					flag = CleanTool.cleanS(original, patternCard, cardReg, "idCard", "Contact");
				}
			}
		} else {
			if (original.containsKey("idCard")) {
				String idCard = (String) original.get("idCard");
				if (CleanUtil.matchIdCard(idCard)) {
					CleanTool.clean(original, "idCard", idCard, patternCard, cardReg);
				} else {
					flag = CleanTool.cleanT(original, "idCard", patternCard, cardReg, "Contact");
				}
			} else {
				flag = CleanTool.cleanS(original, patternCard, cardReg, "idCard", "Contact");
			}
		}
		// idCardc都不满足
		if (!flag) {
			CleanTool.cleanK(original, "idCard");
		}
		boolean tool = true;
		// **********************************************phone******************************************
		if (original.containsKey("phone")) {
			String phone = (String) original.get("phone");
			if (CleanUtil.matchPhone(phone)) {

				CleanTool.clean(original, "phone", phone, patternPhone, phoneReg);
			} else {
				tool = CleanTool.cleanT(original, "phone", patternPhone, phoneReg, "Contact");
			}
		} else {
			CleanTool.cleanS(original, patternPhone, phoneReg, "phone", "Contact");
		}
		// 存在phone但是都不满足
		if (!tool) {
			CleanTool.cleanK(original, "phone");
		}
		// **********************************************homeCall******************************************
		boolean call = true;
		if (original.containsKey("homeCall")) {
			String homeCall = (String) original.get("homeCall");
			if (CleanUtil.matchCall(homeCall)) {
				CleanTool.clean(original, "homeCall", homeCall, patternCall, callReg);
			} else {
				call = CleanTool.cleanT(original, "homeCall", patternCall, callReg, "Contact");
			}
		} else {
			CleanTool.cleanS(original, patternCall, callReg, "homeCall", "Contact");
		}
		if (!call) {
			CleanTool.cleanK(original, "homeCall");
		}
		// 处理空字段
		CleanTool.cleanNull(original);
		if (original.containsKey("zipCode")) {
			if (!CleanUtil.matchNum(String.valueOf(original.get("zipCode")))) {
				CleanTool.cleanK(original, "zipCode");
			}
		}
		if (original.containsKey("name")) {
			if (!CleanUtil.matchChinese(String.valueOf(original.get("name")))
					&& !CleanUtil.matchEnglish(String.valueOf(original.get("name")))) {
				CleanTool.cleanK(original, "name");
			}
		}

		// 判断数据是否符合要求
		flag = CleanTool.cleanX(original);
		if (flag) {
			correct.putAll(CleanTool.Transitio(original));
		} else {
			incorrect.putAll(erro);
		}
	}
}
