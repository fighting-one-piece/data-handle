package org.platform.modules.mapreduce.clean.xx.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.dictionary.DictionaryFactory;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.bson.Document;
import org.platform.modules.mapreduce.base.BaseHDFS2MongoJob;
import org.platform.modules.mapreduce.base.BaseHDFS2MongoV2Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;
import org.platform.utils.IDGenerator;

public class Test extends BaseHDFS2MongoJob {

	@Override
	public Class<? extends Mapper<LongWritable, Text, NullWritable, Text>> getMapperClass() {
		return AnalysisAddressJob2Mapper.class;
	}

	public static void main(String[] args) {
		try {
			args = new String[] { "test", "test","10","1000","hdfs://192.168.0.115:9000/user/xx/test/logistics" };
			ToolRunner.run(new Test(), args);
//			List<String> files = new ArrayList<String>();
//			HDFSUtils.readAllFiles(args[2], "^(logistics)+.*$", files);
//			StringBuilder sb = new StringBuilder();
//			for (int i = 0, len = files.size(); i < len; i++) {
//				sb.append(files.get(i)).append(",");
//				//if ((i % 1 == 0 || i == (len - 1)) && i != 0) {
//					sb.deleteCharAt(sb.length() - 1);
//					String[] str = new String[] { args[0], args[1], "8000", "500", sb.toString() };
//					System.out.println("i>>" + i);
//					System.out.println("路径:>>" + sb.toString());
//					if (i >= 5)
//						break;
//					sb = new StringBuilder();
//				//}
//			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class AnalysisAddressJob2Mapper extends BaseHDFS2MongoV2Mapper {
	private static final String DELIMITER = "\\$#\\$";

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		WordConfTools.set("dic.path", "classpath:dic.txt,"
				+ Test.class.getClassLoader().getResource("mapping/ljp/dic.txt").getFile().replaceFirst("/", ""));
		DictionaryFactory.reload();
	}

	@Override
	protected void buildDocument(Map<String, Object> record, List<Document> documents) {
		System.out.println("进入map阶段");
		String province = null, city = null, county = null, address = null, linkProvince = null, linkCity = null,
				linkCounty = null, linkAddress = null;
		boolean flag = true;
		List<Word> list = new ArrayList<Word>();
		List<Word> listlinkAddress = new ArrayList<Word>();

		List<String> provinceList = Arrays.asList(DataCleanUtils.indexReadSource("ljp/province.txt").split(DELIMITER));
		List<String> cityList = Arrays.asList(DataCleanUtils.indexReadSource("ljp/city.txt").split(DELIMITER));
		List<String> countyList = Arrays.asList(DataCleanUtils.indexReadSource("ljp/county.txt").split(DELIMITER));
		System.out.println(bool(record, "address"));
		if (bool(record, "address"))
			list = WordSegmenter.seg((String) record.get("address"));
		if (bool(record, "linkAddress"))
			listlinkAddress = WordSegmenter.seg((String) record.get("linkAddress"));
		System.out.println(list + "list");
		for (int i = 0; i < provinceList.size(); i++) {
			province = bool(record, provinceList, i, "province") ? (String) record.get("province")
					: listsize(list, province, provinceList, i);
			linkProvince = bool(record, provinceList, i, "province") ? (String) record.get("linkProvince")
					: listsize(listlinkAddress, linkProvince, provinceList, i);
			if (bool(province, linkProvince)) {
				break;
			}
		}

		for (int i = 0; i < cityList.size(); i++) {
			city = bool(record, cityList, i, "city") ? (String) record.get("city") : listsize(list, city, cityList, i);
			linkCity = bool(record, cityList, i, "linkCity") ? (String) record.get("linkCity")
					: listsize(listlinkAddress, linkCity, cityList, i);
			if (bool(city, linkCity)) {
				break;
			}
		}
		for (int i = 0; i < countyList.size(); i++) {
			county = bool(record, countyList, i, "county") ? (String) record.get("county")
					: listsize(list, county, countyList, i);
			linkCounty = bool(record, countyList, i, "linkCounty") ? (String) record.get("linkCounty")
					: listsize(listlinkAddress, linkCounty, countyList, i);
			if (bool(county, linkCounty)) {
				break;
			}
		}

		address = bool(record, "address") ? (String) record.get("address")
				: (province + city + county).replaceAll("null", "");
		linkAddress = bool(record, "linkAddress") ? (String) record.get("linkAddress")
				: (linkProvince + linkCity + linkCounty).replaceAll("null", "");
		if ((province == null || "".equals(province)) && (city == null || "".equals(city))
				&& (county == null || "".equals(county)) && (address == null || "".equals(address))
				&& (linkProvince == null || "".equals(linkProvince)) && (linkCity == null || "".equals(linkCity))
				&& (linkCounty == null || "".equals(linkCounty)) && (linkAddress == null || "".equals(linkAddress))) {
			flag = false;
		}

		if (flag) {
			System.out.println("province：" + province);
			System.out.println("city：" + city);
			System.out.println("county：" + county);
			System.out.println("address：" + address);
			Document document1 = new Document();
			document1.put("province", province);
			document1.put("city", city);
			document1.put("county", county);
			document1.put("address", address);
			String id = IDGenerator.generateByMapValues(document1);
			document1.put("_id", id);
			documents.add(document1);
			Document document2 = new Document();
			document2.put("province", linkProvince);
			document2.put("city", linkCity);
			document2.put("county", linkCounty);
			document2.put("address", linkAddress);
			String linkId = IDGenerator.generateByMapValues(document2);
			document2.put("_id", linkId);
			documents.add(document2);
		}
	}

	private static boolean bool(Map<String, Object> record, String index) {
		return record.containsKey(index) && CleanUtil.matchChinese((String) record.get(index))
				&& !"".equals((String) record.get(index)) && (String) record.get(index) != null;
	}

	private static boolean bool(String index, String linkIndex) {
		return (!"".equals(index) && null != index && !"NA".equals(index))
				|| (!"".equals(linkIndex) && null != linkIndex && !"NA".equals(linkIndex));
	}

	private static boolean bool(Map<String, Object> record, List<String> countyList, int i, String index) {
		return record.containsKey(index) && (((String) record.get(index)).contains(countyList.get(i))
				&& !"".equals((String) record.get(index)) && (String) record.get(index) != null)
				&& !CleanUtil.matchNum((String) record.get(index));
	}

	public static String listsize(List<Word> list, String province, List<String> provinceList, int i) {
		if (list.size() <= 1 && list.size() > 0) {
			province = list.get(0).toString().contains(provinceList.get(i)) ? list.get(0).toString() : "NA";
		} else if (list.size() <= 2 && list.size() > 1) {
			province = list.get(0).toString().contains(provinceList.get(i)) ? list.get(0).toString()
					: list.get(1).toString().contains(provinceList.get(i)) ? list.get(1).toString() : "NA";
		} else if (list.size() >= 3) {
			province = list.get(0).toString().contains(provinceList.get(i)) ? list.get(0).toString()
					: list.get(1).toString().contains(provinceList.get(i)) ? list.get(1).toString()
							: list.get(2).toString().contains(provinceList.get(i)) ? list.get(2).toString() : "NA";
		}
		return province;
	}

}
