package org.platform.modules.mapreduce.clean.ljp.HDFS2HDFS.V2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV2Mapper;
import org.platform.modules.mapreduce.clean.ljp.publicMethods.logisticsCleanMapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 新版TXT清洗规则V2,分隔符式新版清洗 修改参数: 1.输入路径 2.输出路径 3.循环数的更改
 * 
 * @author Administrator
 *
 */
public class FinancialLogisticsHDFS2HDFSV2Job extends BaseHDFS2HDFSJob {
	private static Logger LOG = LoggerFactory.getLogger(FinancialLogisticsHDFS2HDFSV2Job.class);

	@Override
	public Class<? extends BaseHDFS2HDFSV2Mapper> getMapperClass() {
		return LogisticsHDFS2HDFSMapper.class;
	}

	public static void main(String[] args) {
		List<String> files = new ArrayList<String>();
		int exitCode = 0;
		try {
			HDFSUtils.readAllFiles(args[0], "^(logistics)+.*$", files);
			for (int i = 261, len = files.size(); i < len; i++) {
				String path = files.get(i);
				String line = DataCleanUtils.readAndDetectionFirstLine(path, "logistics");
				if (line != null) {
					String output = path.substring(path.lastIndexOf("/"));
					String[] str = new String[] { line, path, args[1] + output };
					System.out.println(i+">>>i");
					ToolRunner.run(new FinancialLogisticsHDFS2HDFSV2Job(), str);
				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
}

class LogisticsHDFS2HDFSMapper extends BaseHDFS2HDFSV2Mapper {
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
		if (corriSize > 5 && DataCleanUtils.judgeSpecialField(original)) {
			correct.putAll(logisticsCleanMapper.replaceMap(logisticsCleanMapper.keyReplace(original)));
		} else {
			incorrect.putAll(map);
		}
	}
}