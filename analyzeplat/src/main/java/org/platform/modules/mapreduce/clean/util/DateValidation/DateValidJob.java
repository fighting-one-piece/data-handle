package org.platform.modules.mapreduce.clean.util.DateValidation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

public class DateValidJob {

	@SuppressWarnings({"finally"})
	public static String FormatDate(String startTime) {
		String dateStr = startTime.replace("/", "-").replace(".", "-").replace(",", "-").replace("时", ":").replace("分", ":")
				.replace("秒", "");
		HashMap<String, String> dateRegFormat = new HashMap<String, String>();
		/** yyyy-MM-dd-HH-mm-ss 格式的时间 */
		dateRegFormat.put("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$",
				"yyyy-MM-dd-HH-mm-ss");// 2014年3月12日 13时5分34秒，2014-03-12
										// 12:05:34，2014/3/12 12:5:34
		/** yyyy-MM-ddHH-mm-ss */
		dateRegFormat.put(
				"(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8])))(([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[13579][26])00))-02-29)",
				"yyyy-MM-ddHH-mm-ss");// 2014/3/1212:5:34
		/** dd-MM-yy-HH-mm-ss */
		dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$",
				"dd-MM-yy-HH-mm-ss");// 21-12-15 15:57:27
		/** dd-MM-yyyy-HH-mm-ss */
		dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$",
				"dd-MM-yyyy-HH-mm-ss");
		/** MM-dd-yy */
		dateRegFormat.put(
				"^(((0[1-9]|1[0-2])[,-\\.]{1}(0[1-9]|1[0-9]|2[0-8])|(0[13-9]|1[0-2])[,-\\.]{1}(29|30)|(0[13578]|1[02])"
						+ "[,-\\.]{1}31)[,-\\.]{1}(?!0000)[0-9]{2}|02[,-\\.]{1}29[,-\\.]{1}([0-9]{2}(0[48]|[2468][048]|[13579][26])|(0[48]|[2468][048]|[13579][26])00))$",
				"MM-dd-yy");// 21-12-14
		/** yyyy-MM-dd-HH-mm */
		dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd-HH-mm");// 2014-03-12
																									// 12:05
		/** yyyy-MM-dd-HH */
		dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd-HH");// 2014-03-12
																						// 12
		/** yyyy-MM-dd */
		dateRegFormat.put("(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[13579][26])00))-02-29)$", "yyyy-MM-dd");// 2014-03-12
		/** "yyyy-MM-d */
		dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{1}$", "yyyy-MM-d");// 2014-03-2
		/** yyyy-M-dd */
		dateRegFormat.put("^\\d{4}\\D+\\d{1}\\D+\\d{2}$", "yyyy-M-dd");// 2014-3-12
		/** yyyy-M-d */
		dateRegFormat.put("^\\d{4}\\D+\\d{1}\\D+\\d{1}$", "yyyy-M-d");// 2014-3-2
		/** yyyy年MM月dd日 */
		dateRegFormat.put("[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日", "yyyy年MM月dd日");// 2014-03-12
		/** yyyy-MM */
		dateRegFormat.put("^\\d{4}\\D+\\d{1,2}$", "yyyy-MM");// 2014-03
		/** yyyy年MM月 */
		dateRegFormat.put("[0-9]{4}年[0-9]{1,2}月", "yyyy年MM月");// 2014年03月
		/** yyyy */
		dateRegFormat.put("^\\d{4}$", "yyyy");// 2014
		/** yyyy年 */
		dateRegFormat.put("[0-9]{4}年", "yyyy年");// 2014年
		/** yyyyMMddHHmmss */
		dateRegFormat.put("^\\d{14}$", "yyyyMMddHHmmss");// 20140312120534
		/** yyyyMMddHHmm */
		dateRegFormat.put(
				"(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})(((0[13578]|1[02])(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)(0[1-9]|[12][0-9]|30))|(02(0[1-9]|[1][0-9]|2[0-8])))(([0-1]?[0-9]|2[0-3])([0-5][0-9])))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[13579][26])00))-02-29)",
				"yyyyMMddHHmm");// 201403121205*/
		/** 将Unix时间戳格式日期 转换为 yyyy-MM-dd-HH-mm-ss */
		dateRegFormat.put("^\\d{10}$", "yyyy-MM-dd-HH-mm-ss");// Unix时间戳格式日期
		dateRegFormat.put("^\\d{11}$", "yyyy-MM-dd-HH-mm-ss");
		dateRegFormat.put("^\\d{12}$", "yyyy-MM-dd-HH-mm-ss");
		dateRegFormat.put("^\\d{13}$", "yyyy-MM-dd-HH-mm-ss");
		/** yyyyMMddHH */
		dateRegFormat.put(
				"((19[6789]{1}\\d{1})|(200\\d{1})|(201[0-7]{1}))((0[0-9]{1})|(1[012]{1}))(([012]{1}[0-9{1}])|3[01]{1})((0\\d{1})|(1\\d{1})|(2[01234]{1}))",
				"yyyyMMddHH");// 2014031212
		/** yyyyMMdd */
		dateRegFormat.put("^\\d{8}$", "yyyyMMdd");// 20140312
		/** yyyyMM */
		dateRegFormat.put("^\\d{6}$", "yyyyMM");// 201403
		/** HH-mm-ss */
		dateRegFormat.put("^([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$", "HH-mm-ss");// 13:05:34  拼接当前日期
		/** yyyy-MM-dd-HH-mm */
		dateRegFormat.put("^\\d{2}\\s*:\\s*\\d{2}$", "yyyy-MM-dd-HH-mm");// 13:05
		/** yy-MM-dd */
		dateRegFormat.put(
				"^(([7890]{1}[0-9]{1})|(1[0-7]{1}))[-\\.\\s]+((0[0-9]{1})|(1[012]{1})|\\d{1})[-\\.\\s]+(([012]{1}[0-9]{1})|(3[01]{1})|(\\d{1}))",
				"yy-MM-dd");// 14.10.18(年.月.日)
		/** yyyy-dd-MM */
		dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}$", "yyyy-dd-MM");// 30.12(日.月)
		/** dd-MM-yy */
		dateRegFormat.put(
				"(([012]{1}[0-9]{1})|(3[01]{1})|(\\d{1}))[-\\.\\s]+((0[0-9]{1})|(1[012]{1})|\\d{1})[-\\.\\s]+(([7890]{1}[0-9]{1})|(1[0-7]{1}))",
				"dd-MM-yy");// 12.21.2013(日.月.年)
		/** dd-MM-yyyy */
		dateRegFormat.put("[0-9]{1,2}\\D+[0-9]{1,2}\\D+[0-9]{4}$", "dd-MM-yyyy");// 12.21.2013(日.月.年)
		// String curDate = null;
		DateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat formatter2;
		DateFormat formatter3 = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat formatter4 = new SimpleDateFormat("yyyy-MM");
		DateFormat formatter5 = new SimpleDateFormat("yyyy");	
		String dateReplace;
		String strSuccess = "";
		boolean flag = false;
		if (dateStr.contains("年")&&dateStr.contains("月")&&dateStr.contains("日")&&dateStr.matches("[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日")) {
			dateStr = dateStr.replace("年", "-").replace("月", "-").replace("日", "");
			flag = true;
		}else if (dateStr.contains("年")&&(dateStr.contains("月")&&dateStr.matches("[0-9]{4}年[0-9]{1,2}月"))) {
			dateStr = dateStr.replace("年", "-").replace("月", "");
			flag = true;
		}else if (dateStr.contains("年")&&dateStr.matches("[0-9]{4}年")) {
			dateStr = dateStr.replace("年", "");
			flag = true;
		}  
		
		try {
			for (String key : dateRegFormat.keySet()) {
				if (Pattern.compile(key).matcher(dateStr).matches()) {
					formatter2 = new SimpleDateFormat(dateRegFormat.get(key));	
						dateReplace = dateStr.replaceAll("\\D+", "-").replace("/", "-");
						if ((key.equals("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$")
								|| key.equals("(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[13579][26])00))-02-29)$")
								|| key.equals(
										"((19[6789]{1}\\d{1})|(200\\d{1})|(201[0-7]{1}))((0[0-9]{1})|(1[012]{1}))(([012]{1}[0-9{1}])|3[01]{1})((0\\d{1})|(1\\d{1})|(2[01234]{1}))")
								|| key.equals("^\\d{8}$")
								|| key.equals(
										"^(([7890]{1}[0-9]{1})|(1[0-7]{1}))[-\\.\\s]+((0[0-9]{1})|(1[012]{1})|\\d{1})[-\\.\\s]+(([012]{1}[0-9]{1})|(3[01]{1})|(\\d{1}))")
								|| key.equals(
										"^(((0[1-9]|1[0-2])[,-\\.]{1}(0[1-9]|1[0-9]|2[0-8])|(0[13-9]|1[0-2])[,-\\.]{1}(29|30)|(0[13578]|1[02])[,-\\.]{1}31)[,-\\.]{1}(?!0000)[0-9]{2}|02[,-\\.]{1}29[,-\\.]{1}([0-9]{2}(0[48]|[2468][048]|[13579][26])|(0[48]|[2468][048]|[13579][26])00))$")
								|| key.equals("[0-9]{1,2}\\D+[0-9]{1,2}\\D+[0-9]{4}$")
								|| key.equals("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{4}$")
								|| key.equals("^\\d{4}\\D+\\d{2}\\D+\\d{1}$")
								|| key.equals("^\\d{4}\\D+\\d{1}\\D+\\d{2}$")
								|| key.equals("^\\d{4}\\D+\\d{1}\\D+\\d{1}$")
								|| key.equals(
										"(([012]{1}[0-9]{1})|(3[01]{1})|(\\d{1}))[-\\.\\s]+((0[0-9]{1})|(1[012]{1})|\\d{1})[-\\.\\s]+\\d{2}")
								|| key.equals(
										"(([012]{1}[0-9]{1})|(3[01]{1})|(\\d{1}))[-\\.\\s]+((0[0-9]{1})|(1[012]{1})|\\d{1})[-\\.\\s]+(([7890]{1}[0-9]{1})|(1[0-7]{1}))"))) {
							strSuccess = formatter3.format(formatter2.parse(dateReplace));
							flag = true;
						} else if (key.equals("^\\d{4}\\D+\\d{1,2}$") || key.equals("^\\d{6}$")
								|| key.equals("^\\d{4}\\D+\\d{2}$\\D")) {
							strSuccess = formatter4.format(formatter2.parse(dateStr));
							flag = true;
						} else if (key.equals("^\\d{10}$") || key.equals("^\\d{11}$") || key.equals("^\\d{12}$")
								|| key.equals("^\\d{13}$")) {
							strSuccess = conversionDateFormat(dateStr);
							flag = true;
						} else if (key.equals("^([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$")) {
							strSuccess = dateStr;
							flag = true;
						}else if (key.equals("^\\d{4}$")){
							strSuccess = formatter5.format(formatter2.parse(dateStr));
							flag = true;
						}else {
							strSuccess = formatter1.format(formatter2.parse(dateReplace));
							flag = true;
						}
                    break;
				} 
			}

		} catch (Exception e) {
			// System.err.println("-----------------日期格式无效:" + dateStr);
			//LOG.error(e.getMessage(), e);
		} finally {
			if (flag && (strSuccess.matches("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$")
					||strSuccess.matches("(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29)$")
					||strSuccess.matches("^\\d{4}\\D+\\d{1,2}$")
					||strSuccess.matches("^\\d{4}$"))) { 
				return strSuccess;
			}else{
				return dateStr;
			}
		}
	}
	/**
	 * Unix日期编码格式的转换
	 * 
	 * @param Date
	 * @return
	 * @throws Exception 
	 */
	private static String conversionDateFormat(String Date) throws Exception {
		SimpleDateFormat myFmt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuilder sbuilder = new StringBuilder(30);
		String dateFormat = null;
		try {
			for (String string : Date.split("[^\\d]")) {
				sbuilder.append(string);
			}
			switch (sbuilder.length()) {
			case 10:
				sbuilder = sbuilder.append("000");
				dateFormat = myFmt2.format(new Date(Long.parseLong(sbuilder.toString())));
				break;
			case 11:
				sbuilder = sbuilder.append("00");
				dateFormat = myFmt2.format(new Date(Long.parseLong(sbuilder.toString())));
				break;
			case 12:
				sbuilder = sbuilder.append("0");
				dateFormat = myFmt2.format(new Date(Long.parseLong(sbuilder.toString())));
				break;
			case 13:
				dateFormat = myFmt2.format(new Date(Long.parseLong(sbuilder.toString())));
				break;
			}
		} catch (Exception e) {
			throw new Exception("日期格式无效");
		}
		return dateFormat;
	}
	
			
}
			


