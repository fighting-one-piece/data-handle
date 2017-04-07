package org.platform.modules.mapreduce.clean.xx.hdfs2hdfs.v1;

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
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.xx.util.CleanTool;

/**
 * 通讯录清洗
 * 
 * @author Administrator
 *
 */
public class WorkElderHDFS2HDFSJob2 extends BaseHDFS2HDFSJob {

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return WorkElderHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		try {
			for (int i = 0; i <= 9; i++) {
				String fss = "hdfs://192.168.0.10:9000/elasticsearch_original/work/elder/20/records-1-m-0000" + i;
				Configuration conf = new Configuration();
				FileSystem hdfs;
				hdfs = FileSystem.get(URI.create(fss), conf); // 通过uri来指定要返回的文件系统
				FileStatus[] fs = hdfs.listStatus(new Path(fss)); // FileStatus
																	// 封装了hdfs文件和目录的元数据
				Path[] listPath = FileUtil.stat2Paths(fs); // 将FileStatus对象转换成一组Path对象
				for (Path p : listPath) {
					if (!p.getName().equals("_SUCCESS") && !p.getName().equals("part-r-00000")) {
						args = new String[] { "", fss, "hdfs://192.168.0.10:9000/elasticsearch_clean_1/work/elder/20/" + p.getName() };
						ToolRunner.run(new WorkElderHDFS2HDFSJob2(), args);
					}
				}
			}

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class WorkElderHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

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
		if (original.containsKey("idCard")) {
			String idCard = (String) original.get("idCard");
			if (CleanUtil.matchIdCard(idCard)) {
				CleanTool.clean(original, "idCard", idCard, patternCard, cardReg);
			} else {
				flag = CleanTool.cleanT(original, "idCard", patternCard, cardReg, "elder");
			}
		} else {
			flag = CleanTool.cleanS(original, patternCard, cardReg, "idCard", "elder");
		}

		// idCard都不满足
		if (!flag) {
			CleanTool.cleanK(original, "idCard");
		}
		boolean tool = true;
		// **********************************************phone******************************************
		if (original.containsKey("mobilePhone")) {
			String phone = (String) original.get("mobilePhone");
			if (CleanUtil.matchPhone(phone)) {
				CleanTool.clean(original, "mobilePhone", phone, patternPhone, phoneReg);
			} else {
				tool = CleanTool.cleanT(original, "mobilePhone", patternPhone, phoneReg, "elder");
			}
		} else {
			CleanTool.cleanS(original, patternPhone, phoneReg, "mobilePhone", "elder");
		}
		// 存在phone但是都不满足
		if (!tool) {
			CleanTool.cleanK(original, "mobilePhone");
		}
		// **********************************************homeCall******************************************
		boolean call = true;
		if (original.containsKey("homeCall")) {
			String homeCall = (String) original.get("homeCall");
			if (CleanUtil.matchCall(homeCall)) {
				CleanTool.clean(original, "homeCall", homeCall, patternCall, callReg);
			} else {
				call = CleanTool.cleanT(original, "homeCall", patternCall, callReg, "elder");
			}
		} else {
			CleanTool.cleanS(original, patternCall, callReg, "homeCall", "elder");
		}
		if (!call) {
			CleanTool.cleanK(original, "homeCall");
		}
		// 处理空字段
		CleanTool.cleanNull(original);
		if (original.containsKey("name")) {
			if (!CleanUtil.matchChinese(String.valueOf(original.get("name")))
					&& !CleanUtil.matchEnglish(String.valueOf(original.get("name")))) {
				CleanTool.cleanK(original, "name");
			}
		}
		if (original.containsKey("address")) {
			String address = (String) original.get("address");
			if (CleanUtil.matchNum(address)||(!CleanUtil.matchEnglish(address)&&!CleanUtil.matchChinese(address))) {
				CleanTool.cleanK(original, "address");
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