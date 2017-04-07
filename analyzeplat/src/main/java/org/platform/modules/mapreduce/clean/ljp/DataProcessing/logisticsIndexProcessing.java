package org.platform.modules.mapreduce.clean.ljp.DataProcessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.platform.modules.mapreduce.clean.util.CleanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class logisticsIndexProcessing {
	private static Logger LOG = LoggerFactory.getLogger(logisticsIndexProcessing.class);
	public static void main(String[] args) {
		
		InputStream in = null;
		BufferedReader br = null;
		OutputStream out = null;
		BufferedWriter bw = null;
		boolean flag = true;
		List<String> list = new ArrayList<String>();
		try {
			in = new FileInputStream(new File("D:\\DataAnalyze\\xxx.txt"));
			br = new BufferedReader(new InputStreamReader(in));
			out = new FileOutputStream(new File("D:\\DataAnalyze\\DataExport.txt"));
			bw = new BufferedWriter(new OutputStreamWriter(out));
			String line = null;
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {
				list = Arrays.asList(line.split("\t"));
				for(int i=0;i<list.size();i++){
					if(list.get(i).contains(readSource("city.txt").replaceAll("\"", ""))
							||list.get(i).contains(readSource("province.txt").replaceAll("\"", ""))
							||list.get(i).contains(readSource("county.txt").replaceAll("\"", ""))) flag = false;
				sb.append(CleanUtil.matchPhone(list.get(i))?list.get(i):"")
				.append(",").append(CleanUtil.matchCall(list.get(i))?list.get(i):"")
				.append(",").append(CleanUtil.matchChinese(list.get(i))
						&&list.get(i).length()<=5&&flag?list.get(i):"")
				.append(",").append(readSource("province.txt").replaceAll("\"", "").contains(list.get(i))?list.get(i):"")
				.append(",").append(readSource("city.txt").replaceAll("\"", "").contains(list.get(i))?list.get(i):"")
				.append(",").append(readSource("county.txt").replaceAll("\"", "").contains(list.get(i))?list.get(i):"")
				.append(",").append(CleanUtil.matchChinese(list.get(i))&&list.get(i).length()>=6?list.get(i):"");
				}
				list = new ArrayList<String>();
			}
			bw.write(sb.toString());
			bw.newLine();
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in)
					in.close();
				if (null != br)
					br.close();
				if (null != out)
					out.close();
				if (null != bw)
					bw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 根据文件名称读取mapping文件
	 * 
	 * @param fileme
	 * @return
	 */
	private static String readSource(String fileme) {
		InputStream in = null;
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = logisticsIndexProcessing.class.getClassLoader().getResourceAsStream("mapping/" + fileme);
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
	
	public void test(){
		String line = "941108	840991	0	0	0	0	湖南省湘潭市雨湖区南盘岭香山雅墅8号	邹紫璇	18607328848	07328999998	null	null	0	12238	2015-04-02 16:13:48	1	500000";
		boolean flag = true;
		StringBuilder sb = new StringBuilder();
		List<String> list = new ArrayList<String>();
		list = Arrays.asList(line.split("\t"));
		for(int i=0;i<list.size();i++){
			if(list.get(i).contains(readSource("city.txt").replaceAll("\"", ""))
					||list.get(i).contains(readSource("province.txt").replaceAll("\"", ""))
					||list.get(i).contains(readSource("county.txt").replaceAll("\"", ""))) flag = false;
			System.out.println(list.get(i)+"list.get(i)");
		sb.append(CleanUtil.matchPhone(list.get(i))?list.get(i):"")
		.append(",").append(CleanUtil.matchCall(list.get(i))?list.get(i):"")
		.append(",").append(CleanUtil.matchChinese(list.get(i))
				&&list.get(i).length()<=5&&flag?list.get(i):"")
		.append(",").append(readSource("province.txt").replaceAll("\"", "").contains(list.get(i))?list.get(i):"")
		.append(",").append(readSource("city.txt").replaceAll("\"", "").contains(list.get(i))?list.get(i):"")
		.append(",").append(readSource("county.txt").replaceAll("\"", "").contains(list.get(i))?list.get(i):"")
		.append(",").append(CleanUtil.matchChinese(list.get(i))&&list.get(i).length()>=6?list.get(i):"");
		}
		System.out.println(sb.toString());
	}
}
