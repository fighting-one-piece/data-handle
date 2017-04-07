package org.platform.modules.mapreduce.clean.util.DateValidation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtil {
	/** 匹配格式 yyyy/MM/dd yyyy.MM.dd yyyy-MM-dd yyyy,MM,dd */
	public static final String DateFromRex = "\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\/\\d{1,2}\\/\\d{1,2}|\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\.\\d{1,2}\\.\\d{1,2}|\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\-\\d{1,2}\\-\\d{1,2}|\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\,\\d{1,2}\\,\\d{1,2}";
	/** 匹配格式dd-MM-y[2|4] dd/MM/y[2|4] ddMMy[2|4] */
	public static final String DateFromRex2 = "\\d{1,2}\\-\\d{1,2}\\-\\d{0,1}[0-7]{1}|\\d{1,2}\\/\\d{1,2}\\/\\d{0,1}[0-7]{1}|\\d{1,2}\\d{1,2}\\d{0,1}[0-7]{1}|\\d{1,2}\\.\\d{1,2}\\.\\d{0,1}[0-7]{1}|\\d{1,2}\\,\\d{1,2}\\,\\d{0,1}[0-7]{1}|\\d{1,2}\\-\\d{1,2}\\-\\d{3}[0-7]{1}|\\d{1,2}\\/\\d{1,2}\\/\\d{3}[0-7]{1}|\\d{1,2}\\d{1,2}\\d{3}[0-7]{1}|\\d{1,2}\\.\\d{1,2}\\.\\d{3}[0-7]{1}|\\d{1,2}\\,\\d{1,2}\\,\\d{3}[0-7]{1}";
	/** 匹配格式yyyy-MM yyyy/MM yyyy.MM yyyy,MM */
	public static final String DateFromRex3 = "\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\/\\d{1,2}|\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\-\\d{1,2}|\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\.\\d{1,2}|\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}\\,\\d{1,2}";
	/** 匹配格式dd.MM.yyyy dd-MM-yyyy dd,MM,yyyy dd/MM/yyyy */
	public static final String DateFromRex4 = "\\d{1,2}\\/\\d{1,2}\\/\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|\\d{1,2}\\.\\d{1,2}\\.\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|\\d{1,2}\\-\\d{1,2}\\-[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3}|\\d{1,2}\\,\\d{1,2}\\,\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}";
	/** 匹配格式MM-yyyy MM,yyyy MM/yyyy MM.yyyy */
	public static final String DateFromRex5 = "\\d{1,2}\\/[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3}|\\d{1,2}\\-\\[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|\\d{1,2}\\.[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3}|\\d{1,2}\\,[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3}";
	/** 去除中文 */
	public static final String RemoveChineseRex = "[\u4e00-\u9fa5]";
	/** 匹配yyyy年mm月dd日*/
	public static final String YearMonthDayRex="[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日";
	/** 匹配yyyy年mm月 */
	public static final String YearMonthRex ="[0-9]{4}年[0-9]{1,2}月";
	/** 匹配yyyy年 */
	public static final String YearRex ="[0-9]{4}年";

	/***
	 * 以下为发现符合正则表达式的值
	 * 
	 * @param date
	 * @return
	 */

	public static boolean DateFromFind(String date) {
		Pattern p = Pattern.compile(DateFromRex);
		Matcher m = p.matcher(date);
		return m.find();
	}

	public static boolean DateFromFind2(String date) {
		Pattern p = Pattern.compile(DateFromRex2);
		Matcher m = p.matcher(date);
		return m.find();
	}

	public static boolean DateFromFind3(String date) {
		Pattern p = Pattern.compile(DateFromRex3);
		Matcher m = p.matcher(date);
		return m.find();
	}

	public static boolean DateFromFind4(String date) {
		Pattern p = Pattern.compile(DateFromRex4);
		Matcher m = p.matcher(date);
		return m.find();
	}

	public static boolean DateFromFind5(String date) {
		Pattern p = Pattern.compile(DateFromRex5);
		Matcher m = p.matcher(date);
		return m.find();
	}

	public static boolean YearMonthDayRex(String date) {
		Pattern p = Pattern.compile(YearMonthDayRex);
		Matcher m = p.matcher(date);
		return m.find();
	}
	public static boolean YearMonthRex(String date) {
		Pattern p = Pattern.compile(YearMonthRex);
		Matcher m = p.matcher(date);
		return m.find();
	}
	public static boolean YearRex(String date) {
		Pattern p = Pattern.compile(YearRex);
		Matcher m = p.matcher(date);
		return m.find();
	}
	
	
	/***
	 * 去除值里面的中文
	 * 
	 * @param date
	 * @return
	 */
	public static String RemoveDateChinese(String date) {
		Pattern p = Pattern.compile(RemoveChineseRex);
		Matcher m = p.matcher(date);
		return m.replaceAll("");
	}

	/**
	 * 替换年月日为"-"
	 * 
	 * @param date
	 * @return
	 */
	public static String Replace(String date) {
		if (date.contains("年") && date.contains("月") && date.contains("日")) {
			date = date.replace("年", "-").replace("月", "-").replace("日", "");
		}
		if (date.contains("年") && date.contains("月")) {
			date = date.substring(0, date.indexOf("月")).replace("年", "-").replace("月", "");
		}
		if (date.contains("时") && date.contains("分") && date.contains("秒")) {
			date = date.replace("时", ":").replace("分", ":").replace("秒", "");
		}
		if (date.contains("_") || date.contains(";")) {
			date = date.replace("_", "").replace(";", "");
		}
		return date;
	}


}
