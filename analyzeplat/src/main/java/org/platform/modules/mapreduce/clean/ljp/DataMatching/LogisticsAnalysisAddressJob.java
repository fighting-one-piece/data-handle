package org.platform.modules.mapreduce.clean.ljp.DataMatching;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.bson.Document;
import org.platform.modules.mapreduce.base.BaseHDFS2MongoJob;
import org.platform.modules.mapreduce.base.BaseHDFS2MongoV2Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;
import org.platform.utils.IDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogisticsAnalysisAddressJob extends BaseHDFS2MongoJob {
	@Override
	public Class<? extends BaseHDFS2MongoV2Mapper> getMapperClass() {
		return LogisticsAnalysisAddressJob2Mapper.class;
	}

	public static void main(String[] args) {
		try {
			args = new String[] { "gis", "address_longitude_latitude",
					"hdfs://192.168.0.10:9000/warehouse_data/financial/logistics" };
			List<String> files = new ArrayList<String>();
			HDFSUtils.readAllFiles(args[2], "^(logistics)+.*$", files);
			StringBuilder sb = new StringBuilder();
			for (int i = 0, len = files.size(); i < len; i++) {
				sb.append(files.get(i)).append(",");
				if ((i % 1 == 0 || i == (len - 1)) && i != 0) {
					sb.deleteCharAt(sb.length() - 1);
					String[] str = new String[] { args[0], args[1], "8000", "500", sb.toString() };
					System.out.println("i>>" + i);
					System.out.println("路径:>>" + sb.toString());
					ToolRunner.run(new LogisticsAnalysisAddressJob(), str);
					if (i >=5)
						break;
					sb = new StringBuilder();
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class LogisticsAnalysisAddressJob2Mapper extends BaseHDFS2MongoV2Mapper {
	private static Logger LOG = LoggerFactory.getLogger(LogisticsAnalysisAddressJob2Mapper.class);

	
	@Override
	protected void buildDocument(Map<String, Object> record, List<Document> documents) {
		String province = null, city = null, county = null, address = null, linkProvince = null, linkCity = null,
				linkCounty = null, linkAddress = null;
		boolean flag = true;
		List<String> provinceList = Arrays
				.asList(readSource("province.txt").replace(" ", "").trim().split(","));
		List<String> cityList = Arrays
				.asList(readSource("city.txt").replace(" ", "").trim().split(","));
		List<String> countyList = Arrays
				.asList(readSource("county.txt").replace(" ", "").trim().split(","));
		for (int i = 0; i < provinceList.size(); i++) {
			if (record.containsKey("province")
					&& (((String) record.get("province")).contains(provinceList.get(i))
							&& !"".equals((String) record.get("province")) && (String) record.get("province") != null)
					&& !CleanUtil.matchNum((String) record.get("province"))) {
				province = (String) record.get("province");
			} else if (record.containsKey("address") && ((String) record.get("address")).contains(provinceList.get(i))
					&& !CleanUtil.matchNum((String) record.get("address")) && !"".equals((String) record.get("address"))
					&& (String) record.get("address") != null) {
				province = provinceList.get(i);
			}
			if (!"".equals(province) && null != province) {
				break;
			}
		}
		for (int i = 0; i < provinceList.size(); i++) {
			if (record.containsKey("linkProvince")
					&& (((String) record.get("linkProvince")).contains(provinceList.get(i))
							&& !"".equals((String) record.get("linkProvince"))
							&& (String) record.get("linkProvince") != null)
					&& !CleanUtil.matchNum((String) record.get("linkProvince"))) {
				linkProvince = (String) record.get("linkProvince");
			} else if (record.containsKey("linkAddress")
					&& ((String) record.get("linkAddress")).contains(provinceList.get(i))
					&& !CleanUtil.matchNum((String) record.get("linkAddress"))
					&& !"".equals((String) record.get("linkAddress")) && (String) record.get("linkAddress") != null) {
				linkProvince = provinceList.get(i);
			}
			if (!"".equals(linkProvince) && null != linkProvince) {
				break;
			}
		}

		for (int i = 0; i < cityList.size(); i++) {
			if (record.containsKey("city")
					&& (((String) record.get("city")).contains(cityList.get(i))
							&& !"".equals((String) record.get("city")) && (String) record.get("city") != null)
					&& !CleanUtil.matchNum((String) record.get("city"))) {
				city = (String) record.get("city");
			} else if (record.containsKey("address") && ((String) record.get("address")).contains(cityList.get(i))
					&& !CleanUtil.matchNum((String) record.get("address")) && !"".equals((String) record.get("address"))
					&& (String) record.get("address") != null) {
				city = cityList.get(i);
			}
			if (!"".equals(city) && null != city) {
				break;
			}
		}
		for (int i = 0; i < cityList.size(); i++) {
			if (record.containsKey("linkCity")
					&& (((String) record.get("linkCity")).contains(cityList.get(i))
							&& !"".equals((String) record.get("linkCity")) && (String) record.get("linkCity") != null)
					&& !CleanUtil.matchNum((String) record.get("linkCity"))) {
				linkCity = (String) record.get("linkCity");
			} else if (record.containsKey("linkAddress")
					&& ((String) record.get("linkAddress")).contains(cityList.get(i))
					&& !CleanUtil.matchNum((String) record.get("linkAddress"))
					&& !"".equals((String) record.get("linkAddress")) && (String) record.get("linkAddress") != null) {
				linkCity = cityList.get(i);
			}
			if (!"".equals(linkCity) && null != linkCity) {
				break;
			}
		}
		for (int i = 0; i < countyList.size(); i++) {
			if (record.containsKey("county")
					&& (((String) record.get("county")).contains(countyList.get(i))
							&& !"".equals((String) record.get("county")) && (String) record.get("county") != null)
					&& !CleanUtil.matchNum((String) record.get("county"))) {
				county = (String) record.get("county");
			} else if (record.containsKey("address") && ((String) record.get("address")).contains(countyList.get(i))
					&& !CleanUtil.matchNum((String) record.get("address")) && !"".equals((String) record.get("address"))
					&& (String) record.get("address") != null) {
				county = countyList.get(i);
			}
			if (!"".equals(county) && null != county) {
				break;
			}
		}
		for (int i = 0; i < countyList.size(); i++) {
			if (record.containsKey("linkCounty")
					&& (((String) record.get("linkCounty")).contains(countyList.get(i))
							&& !"".equals((String) record.get("linkCounty"))
							&& (String) record.get("linkCounty") != null)
					&& !CleanUtil.matchNum((String) record.get("linkCounty"))) {
				linkCounty = (String) record.get("linkCounty");
			} else if (record.containsKey("linkAddress")
					&& ((String) record.get("linkAddress")).contains(countyList.get(i))
					&& !CleanUtil.matchNum((String) record.get("linkAddress"))
					&& !"".equals((String) record.get("linkAddress")) && (String) record.get("linkAddress") != null) {
				linkCounty = countyList.get(i);
			}
			if (!"".equals(linkCounty) && null != linkCounty) {
				break;
			}
		}

		if (record.containsKey("address") && !CleanUtil.matchNum((String) record.get("address"))
				&& !"".equals((String) record.get("address")) && (String) record.get("address") != null) {
			address = (String) record.get("address");
		}
		if (record.containsKey("linkAddress") && !CleanUtil.matchNum((String) record.get("linkAddress"))
				&& !"".equals((String) record.get("linkAddress")) && (String) record.get("linkAddress") != null) {
			linkAddress = (String) record.get("linkAddress");
		}
		if ((province == null || "".equals(province)) && (city == null || "".equals(city))
				&& (county == null || "".equals(county)) && (address == null || "".equals(address))
				&& (linkProvince == null || "".equals(linkProvince)) && (linkCity == null || "".equals(linkCity))
				&& (linkCounty == null || "".equals(linkCounty)) && (linkAddress == null || "".equals(linkAddress))) {
			flag = false;
		}

		if (flag) {
//			 System.out.println(province);
//			 System.out.println(city);
//			 System.out.println(county);
//			 System.out.println(address);
//			 System.out.println("-----------");
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

	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @param fileName
	 * @return
	 */
	private static String readSource(String fileName) {
		InputStream in = null;
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = LogisticsAnalysisAddressJob.class.getClassLoader().getResourceAsStream("mapping/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				sb.append(line);
			}
		} catch (Exception e) {
			LOG.error("read source error.", e);
		} finally {
			try {
				if (null != br) {
					br.close();
				}
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				LOG.error("close reader or stream error.", e);
			}
		}
		return sb.toString();
	}
}