package org.cisiondata.modules.address;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.cisiondata.modules.address.entity.AdministrativeDivision;
import org.junit.Test;

public class AdministrativeDivisionFileParser {

	@Test
	public void testReadDictionaryFile() {
		InputStream in = null;
		BufferedReader br = null;
		OutputStream out1 = null;
		BufferedWriter bw1 = null;
		OutputStream out2 = null;
		BufferedWriter bw2 = null;
		OutputStream out3 = null;
		BufferedWriter bw3 = null;
		OutputStream out4 = null;
		BufferedWriter bw4 = null;
		OutputStream out5 = null;
		BufferedWriter bw5 = null;
		try {
			in = AdministrativeDivisionFileParser.class.getClassLoader().getResourceAsStream("dictionary/administrative_division.csv");
			br = new BufferedReader(new InputStreamReader(in));
			String ad1Path = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_1.dic").getPath();
			out1 = new FileOutputStream(new File(ad1Path));
			bw1 = new BufferedWriter(new OutputStreamWriter(out1));
			String ad2Path = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_2.dic").getPath();
			out2 = new FileOutputStream(new File(ad2Path));
			bw2 = new BufferedWriter(new OutputStreamWriter(out2));
			String ad3Path = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_3.dic").getPath();
			out3 = new FileOutputStream(new File(ad3Path));
			bw3 = new BufferedWriter(new OutputStreamWriter(out3));
			String ad4Path = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_4.dic").getPath();
			out4 = new FileOutputStream(new File(ad4Path));
			bw4 = new BufferedWriter(new OutputStreamWriter(out4));
			String ad5Path = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_5.dic").getPath();
			out5 = new FileOutputStream(new File(ad5Path));
			bw5 = new BufferedWriter(new OutputStreamWriter(out5));
			String line = null;
			while (null != (line = br.readLine())) {
				String[] fields = line.split(",");
				String region = fields[1];
				if ("市辖区".equals(region)) {
					System.out.println(fields[2].substring(2, 4));
					System.out.println(fields[2].substring(4));
				}
				if (fields[2].endsWith("0000000000")) {
					bw1.write(region);
					bw1.newLine();
					if (region.endsWith("省")) {
						bw1.write(region.replace("省", ""));
						bw1.newLine();
					} else if (region.endsWith("市")) {
						bw1.write(region.replace("市", ""));
						bw1.newLine();
					}
				} else if (!fields[2].substring(2, 4).equals("00") && fields[2].substring(4).endsWith("00000000")) {
					bw2.write(region);
					bw2.newLine();
				} else if (!fields[2].substring(4, 6).equals("00") && fields[2].substring(6).endsWith("000000")) {
					bw3.write(region);
					bw3.newLine();
				} else if (!fields[2].substring(6, 9).equals("000") && fields[2].substring(9).endsWith("000")) {
					bw4.write(region);
					bw4.newLine();
				} else if (!fields[2].substring(9).equals("000")) {
					bw5.write(region);
					bw5.newLine();
				}
			}
			bw1.flush();
			bw2.flush();
			bw3.flush();
			bw4.flush();
			bw5.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in) in.close();
				if (null != br) br.close();
				if (null != out1) out1.close();
				if (null != bw1) bw1.close();
				if (null != out2) out2.close();
				if (null != bw2) bw2.close();
				if (null != out3) out3.close();
				if (null != bw3) bw3.close();
				if (null != out4) out4.close();
				if (null != bw4) bw4.close();
				if (null != out5) out5.close();
				if (null != bw5) bw5.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testReadAdministrativeDivision2File() {
		InputStream in = null;
		BufferedReader br = null;
		OutputStream out = null;
		BufferedWriter bw = null;
		try {
			in = AdministrativeDivisionFileParser.class.getClassLoader().getResourceAsStream("dictionary/administrative_division_2.dic");
			br = new BufferedReader(new InputStreamReader(in));
			String ad1Path = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_2_n.dic").getPath();
			out = new FileOutputStream(new File(ad1Path));
			bw = new BufferedWriter(new OutputStreamWriter(out));
			String line = null;
			while (null != (line = br.readLine())) {
				bw.write(line.substring(0, line.length() - 1));
				bw.newLine();
				bw.write(line);
				bw.newLine();
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in) in.close();
				if (null != br) br.close();
				if (null != out) out.close();
				if (null != bw) bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void writeFullAdministrativeDivisionFile() {
		InputStream in = null;
		BufferedReader br = null;
		OutputStream out = null;
		BufferedWriter bw = null;
		try {
			in = AdministrativeDivisionFileParser.class.getClassLoader().getResourceAsStream("dictionary/administrative_division.csv");
			br = new BufferedReader(new InputStreamReader(in));
			String adPath = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division.dic").getPath();
			out = new FileOutputStream(new File(adPath));
			bw = new BufferedWriter(new OutputStreamWriter(out));
			Map<String, String> map1 = new HashMap<String, String>(700000);
			Map<String, String> map2 = new HashMap<String, String>(700000);
			String line = null;
			while (null != (line = br.readLine())) {
				String[] values = line.split(",");
				map1.put(values[2], values[3]);
				map2.put(values[2], values[1]);
			}
			for (Map.Entry<String, String> entry : map1.entrySet()) {
				String code5 = entry.getKey();
				if (!code5.substring(code5.length() - 3).equals("000")) {
					StringBuilder sb = new StringBuilder(map2.get(code5));
					String prefixCode = map1.get(code5);
					sb.append(",").append(map2.get(prefixCode));
					while (!AdministrativeDivision.ROOT.equals(prefixCode)) {
						prefixCode = map1.get(prefixCode);
						if (null != map2.get(prefixCode)) {
							sb.append(",").append(map2.get(prefixCode));
						}
					}
					bw.write(sb.toString());
					bw.newLine();
				}
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in) in.close();
				if (null != br) br.close();
				if (null != out) out.close();
				if (null != bw) bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void writeThreeAdministrativeDivisionFile() {
		InputStream in = null;
		BufferedReader br = null;
		OutputStream out = null;
		BufferedWriter bw = null;
		try {
			in = AdministrativeDivisionFileParser.class.getClassLoader().getResourceAsStream("dictionary/administrative_division.csv");
			br = new BufferedReader(new InputStreamReader(in));
			String adPath = AdministrativeDivisionFileParser.class.getClassLoader()
				.getResource("dictionary/administrative_division_2_3.dic").getPath();
			out = new FileOutputStream(new File(adPath));
			bw = new BufferedWriter(new OutputStreamWriter(out));
			Map<String, String> map1 = new HashMap<String, String>(700000);
			Map<String, String> map2 = new HashMap<String, String>(700000);
			String line = null;
			while (null != (line = br.readLine())) {
				String[] values = line.split(",");
				map1.put(values[2], values[3]);
				map2.put(values[2], values[1]);
			}
			for (Map.Entry<String, String> entry : map1.entrySet()) {
				String code = entry.getKey();
				if (!code.substring(4,6).equals("00") && code.substring(6).equals("000000")) {
					StringBuilder sb = new StringBuilder(map2.get(code));
					String prefixCode = map1.get(code);
					sb.append(",").append(map2.get(prefixCode));
					while (!AdministrativeDivision.ROOT.equals(prefixCode)) {
						prefixCode = map1.get(prefixCode);
						if (null != map2.get(prefixCode)) {
							sb.append(",").append(map2.get(prefixCode));
						}
					}
					bw.write(sb.toString());
					bw.newLine();
				}
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in) in.close();
				if (null != br) br.close();
				if (null != out) out.close();
				if (null != bw) bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
