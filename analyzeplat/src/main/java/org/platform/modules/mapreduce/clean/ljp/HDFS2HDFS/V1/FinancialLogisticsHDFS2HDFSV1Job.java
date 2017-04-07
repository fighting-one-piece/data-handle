package org.platform.modules.mapreduce.clean.ljp.HDFS2HDFS.V1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.ljp.publicMethods.logisticsCleanMapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 旧版清洗规则V1,旧版清洗 修改参数: 1.输入路径 2.输出路径 3.循环数的更改
 * 
 * @author Administrator
 *
 */
public class FinancialLogisticsHDFS2HDFSV1Job extends BaseHDFS2HDFSJob {
	private static Logger LOG = LoggerFactory.getLogger(FinancialLogisticsHDFS2HDFSV1Job.class);

	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return LogisticsHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		List<String> files = new ArrayList<String>();
		int exitCode = 0;
		try {
			args = new String[] {"hdfs://192.168.0.10:9000/warehouse_original/financial/logistics/20ES",
					"hdfs://192.168.0.10:9000/warehouse_clean/financial/logistics/20ES" };
			HDFSUtils.readAllFiles(args[0], "^(records)+.*$", files);
			for (int i = 0, len = files.size(); i < len; i++) {
				String inputPath = files.get(i);
				String output = inputPath.substring(inputPath.lastIndexOf("/"));
				String[] str = new String[] {"",inputPath, args[1] + output };
				System.out.println(i + ">>>i");
				ToolRunner.run(new FinancialLogisticsHDFS2HDFSV1Job(), str);
			}
			System.exit(exitCode);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
}

class LogisticsHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {
	private static String[] string = logisticsCleanMapper.continueArray();
	private static List<String> chineseAndEnglishList = logisticsCleanMapper.chineseAndEnglishList();

	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		original = logisticsCleanMapper.dateFormat(original);
		DataCleanUtils.fieldValidate(original, "idCard", CleanUtil.idCardRex, "", string);
		DataCleanUtils.fieldValidate(original, "linkIdCard", CleanUtil.idCardRex, "", string);
		logisticsCleanMapper.logisticsReplaceSpace(original);
		DataCleanUtils.fieldValidate(original, "phone", CleanUtil.phoneRex, "",
				logisticsCleanMapper.removeList("rcall"));
		DataCleanUtils.fieldValidate(original, "linkPhone", CleanUtil.phoneRex, "",
				logisticsCleanMapper.removeList("linkCall"));
		DataCleanUtils.fieldValidate(original, "idCode", CleanUtil.phoneRex, "",
				logisticsCleanMapper.removeList("rcall"));
		DataCleanUtils.fieldValidate(original, "linkClientCode", CleanUtil.phoneRex, "",
				logisticsCleanMapper.removeList("linkCall"));
		logisticsCleanMapper.phoneExchange(original, "phone", "idCode");
		logisticsCleanMapper.phoneExchange(original, "linkPhone", "linkClientCode");
		DataCleanUtils.fieldValidate(original, "rcall", CleanUtil.callRex, "", string);
		DataCleanUtils.fieldValidate(original, "linkCall", CleanUtil.callRex, "", string);
		DataCleanUtils.fieldValidate(original, "email", CleanUtil.emailRex, "", string);
		DataCleanUtils.fieldValidate(original, "linkEmail", CleanUtil.emailRex, "yes", string);

		/**
		 * 附加判断
		 */

		if (original.containsKey("price")) {
			original = logisticsCleanMapper.priceJudge(original);
		}

		if (original.containsKey("updateTime")) {
			original.put("updateTime", ((String) original.get("updateTime")).replaceAll("\\?|\\？", ""));
		}

		for (String address : chineseAndEnglishList) {
			logisticsCleanMapper.boolChinese(original, address);
		}

		// 判断除NA之外的有效字段
		int corriSize = original.size();
		// 放入结果集,并替换主要字段的key
		System.out.println(original + "----original");
		System.out.println(corriSize > 5 && DataCleanUtils.judgeSpecialField(original));
		if (corriSize > 5 && DataCleanUtils.judgeSpecialField(original)) {
			correct.putAll(logisticsCleanMapper.replaceMap(logisticsCleanMapper.keyReplace(original)));
		} else {
			incorrect.putAll(map);
		}
	}
}