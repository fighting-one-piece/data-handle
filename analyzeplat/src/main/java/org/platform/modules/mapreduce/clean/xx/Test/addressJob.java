package org.platform.modules.mapreduce.clean.xx.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.dictionary.DictionaryFactory;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.platform.modules.mapreduce.clean.ly.divideArea.ProvinceDivideArea;
import org.platform.modules.mapreduce.clean.ly.find.ConditionalSearchJob;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.platform.modules.mapreduce.clean.util.DataCleanUtils;

public class addressJob {
	static final String DELIMITER = "\\$#\\$";
	private static Map<String, String> cityToProvinceMap = readSource("市_省.txt");
	// 加载县市对应的map，县为key
	private static Map<String, String> countyToCityMap = readSource("县_市.txt");

	private static List<String> provinceList = Arrays
			.asList(DataCleanUtils.indexReadSource("ljp/province.txt").split(DELIMITER));
	private static List<String> cityList = Arrays
			.asList(DataCleanUtils.indexReadSource("ljp/city.txt").split(DELIMITER));
	private static List<String> countyList = Arrays
			.asList(DataCleanUtils.indexReadSource("ljp/area.txt").split(DELIMITER));

	public static void main(String[] args) {
		Map<String, Object> record = new HashMap<String, Object>();
		record.put("linkAddress", "河北省沧州市新华区建设北街街道解放中路沧县住宅小区1单1号602");
		//record.put("linkCounty", "锦江区");
		buildDocument(record);
	}

	static {
		// 加载不分词字段
		WordConfTools.set("dic.path", "classpath:dic.txt ," + ProvinceDivideArea.class.getClassLoader()
				.getResource("mapping/ly/dic.txt").toString().replace("file:/", ""));
		DictionaryFactory.reload();

	}

	public static void buildDocument(Map<String, Object> original) {
		try {
			String province = null, city = null, county = null, address = null, linkProvince = null, linkCity = null,
					linkCounty = null, linkAddress = null;

			if (original.containsKey("address") && !CleanUtil.matchNum(address) && !"".equals(address)
					&& (String) original.get("address") != null
					|| original.containsKey("linkAddress") && !CleanUtil.matchNum(linkAddress) && !"".equals(address)
							&& (String) original.get("linkAddress") != null) {
				if (original.containsKey("address")) {
					address = original.get("address").toString().trim().replaceAll(",", "");
				}
				if (original.containsKey("linkAddress")) {
					linkAddress = original.get("linkAddress").toString().trim().replaceAll(",", "");
				}

				// 收件地址
				if (address != null) {
					List<String> list = completeAddress(province, city, county, address, original, "address");

					System.out.println("province：" + list.get(0));
					System.out.println("city：" + list.get(1));
					System.out.println("county：" + list.get(2));
					System.out.println("address：" + list.get(3));
				}
				if (linkAddress != null) {
					List<String> linkList = completeAddress(linkProvince, linkCity, linkCounty, linkAddress, original,
							"linkAddress");

					System.out.println("linkProvince：" + linkList.get(0));
					System.out.println("linkCity：" + linkList.get(1));
					System.out.println("linkCounty：" + linkList.get(2));
					System.out.println("linkAddress：" + linkList.get(3));
				}
			}

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static List<String> completeAddress(String province, String city, String county, String address,
			Map<String, Object> original, String addressType) {
		List<Word> wordsAddress = WordSegmenter.seg(address, SegmentationAlgorithm.MaximumMatching);
		// 开关
		boolean cityStateNull = false;
		boolean provinceStateNull = false;
		boolean countyStateNull = false;

		System.out.println(wordsAddress);
		for (Word word : wordsAddress) {
			String string = word.toString().replaceAll("省", "").replaceAll("市", "");
			// 判断是否有省
			if (provinceList.contains(string)) {
				province = string;
			}
			// 判断是否有市
			if (cityList.contains(string)) {
				city = string;
			}
			// 判断是否有县
			if (countyList.contains(string)) {
				county = string;
			}
			// 根据county字段补全县
			String countyType = null;
			if ("address".equals(addressType)) {
				countyType = "county";
			} else {
				countyType = "linkCounty";
			}
			if (county == null && original.containsKey(countyType) && original.get(countyType) != null) {
				county = original.get(countyType).toString().replaceAll("县", "");
				System.out.println(county);
				if (countyToCityMap.containsKey(county)) {
					// 如果市为空
					if (city == null) {
						city = countyToCityMap.get(county);
						if (cityToProvinceMap.get(city).equals(province)) {
							county = original.get(countyType).toString();
							countyStateNull = true;
						}
					} else if (countyToCityMap.get(county).equals(city)) {
						county = original.get(countyType).toString();
						countyStateNull = true;
					} else {
						county = null;
					}
				} else {
					county = null;
				}
			}
			// 根据县补全市
			if (city == null && county != null) {
				cityStateNull = true;
				city = countyToCityMap.get(county.replaceAll("县", ""));
			}
			// 根据市补全省
			if (province == null && city != null) {
				provinceStateNull = true;
				province = cityToProvinceMap.get(city);
			}
		}
		// 地址拼接
		// 原地址省为空
		if (provinceStateNull == true) {
			address = province + "省" + address;
		}
		// 原地址市和省都为空
		if (cityStateNull == true && provinceStateNull == true && city != null) {
			address = province + "省" + city + "市"
					+ address.substring(address.indexOf(province) + province.length() + 1);
		}
		// 原地址市为空但是省不为空
		if (cityStateNull == true && provinceStateNull == false && city != null) {
			address = province + city + "市" + address.substring(address.indexOf(province) + province.length());
		}
		// 原地址县为空市也为空
		if (countyStateNull == true) {
			if (!address.contains(city)) {
				address = province + "省" + city + "市" + county
						+ address.substring(address.indexOf(province) + province.length() + 1);
			} else {
				address = province + "省" + city + "市" + county
						+ address.substring(address.indexOf(city) + city.length() + 1);
			}

		}
		List<String> list = new ArrayList<String>();
		list.add(province);
		list.add(city);
		list.add(county);
		list.add(address);
		return list;
	}

	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @return
	 */
	public static Map<String, String> readSource(String fileName) {
		Map<String, String> map = new HashMap<String, String>();
		InputStream in = null;
		BufferedReader br = null;
		try {
			in = ConditionalSearchJob.class.getClassLoader().getResourceAsStream("mapping/ly/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				String[] str = line.split("\t");
				if (str.length == 2) {
					map.put(str[1], str[0]);
				} else {
					map.put(str[0], "");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != br) {
					br.close();
				}
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return map;
	}
}
