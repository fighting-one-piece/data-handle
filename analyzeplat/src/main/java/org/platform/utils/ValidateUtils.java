package org.platform.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidateUtils {

	public static final String phoneRex = "1(3[0-9]|4[57]|5[0-35-9]|7[0135678]|8[0-9])(\\d{8}|\\*{4}\\d{4}|\\*{5}\\d{3}|\\*{8})";
	public static final String idCardRex = "\\d{17}(\\d|X|x)|\\d{15}";
	public static final String callRex = "0[0-9]{3}-?[2-9][0-9]{6}|0[0-9]{2}-?[2-9][0-9]{7}|400[0-9]{7}|[2-9][0-9]{6,7}";
	public static final String emailRex = "[\\w!#$%&'*+//=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+//=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?";
	public static final String IPRex = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))";
	public static final String QQRex = "[1-9][0-9]{4,9}";
	/* 验证由数字和26个英文字母组成的字符串 */
	public static final String numAndLetterRex = "^[A-Za-z0-9]+$";
	/* 验证数字 */
	public static final String numRex = "^\\d{1,}$";
	public static final String OCodeRex = "[a-zA-Z0-9]{8}-[a-zA-Z0-9]";
	/* 验证中文 */
	public static final String CNRex = "[\\u4e00-\\u9fa5]+";
	/* 验证浮点数 */
	public static final String doubleRex = "^(([0-9]+\\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\\.[0-9]+)|([0-9]*[1-9][0-9]*))$";
	/* 验证*号 */
	public static final String starRex = "\\*+";
	/* 验证数字 */
	public static final String digitalRex = "-?[1-9]d*";
	/* 验证英文 */
	public static final String englishRex = "[A-Za-z]+";

	/**
	 * 判断字符串中是否包含电话号码
	 * 
	 * @param phone
	 * @return
	 */
	public static boolean matchPhone(String phone) {
		if (phone == null || "".equals(phone)) {
			return false;
		} else {
			Pattern p = Pattern.compile(phoneRex);
			Matcher m = p.matcher(phone);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否包含中文
	 * 
	 * @param str
	 * @return
	 */
	public static boolean matchChinese(String str) {
		if (str == null || "".equals(str)) {
			return false;
		} else {
			Pattern p = Pattern.compile(CNRex);
			Matcher m = p.matcher(str);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否包含身份证号
	 * 
	 * @param idCard
	 * @return
	 */
	public static boolean matchIdCard(String idCard) {
		if (idCard == null || "".equals(idCard)) {
			return false;
		} else {
			Pattern p = Pattern.compile(idCardRex);
			Matcher m = p.matcher(idCard);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否包含座机号
	 * 
	 * @param call
	 * @return
	 */
	public static boolean matchCall(String call) {
		if (call == null || "".equals(call)) {
			return false;
		} else {
			Pattern p = Pattern.compile(callRex);
			Matcher m = p.matcher(call);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否包含邮箱
	 * 
	 * @param call
	 * @return
	 */
	public static boolean matchEmail(String email) {
		if (email == null || "".equals(email)) {
			return false;
		} else {
			Pattern p = Pattern.compile(emailRex);
			Matcher m = p.matcher(email);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否包含IP
	 * 
	 * @param IP
	 * @return
	 */
	public static boolean matchIP(String IP) {
		if (IP == null || "".equals(IP)) {
			return false;
		} else {
			Pattern p = Pattern.compile(IPRex);
			Matcher m = p.matcher(IP);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否包含QQ号
	 * 
	 * @param qq
	 * @return
	 */
	public static boolean matchQQ(String qq) {
		if (qq == null || "".equals(qq)) {
			return false;
		} else {
			Pattern p = Pattern.compile(QQRex);
			Matcher m = p.matcher(qq);
			return m.find();
		}
	}

	/**
	 * 判断字符串中是否是字母与数字的组合
	 * 
	 * @param numAndLetter
	 * @return
	 */
	public static boolean matchNumAndLetter(String str) {
		if (str == null || "".equals(str)) {
			return false;
		} else {
			Pattern p = Pattern.compile(numAndLetterRex);
			Matcher m = p.matcher(str);
			return m.matches();
		}
	}

	/**
	 * 判断字符串中是否是纯数字
	 * 
	 * @param num
	 * @return
	 */
	public static boolean matchNum(String num) {
		if (num == null || "".equals(num)) {
			return false;
		} else {
			Pattern p = Pattern.compile(numRex);
			Matcher m = p.matcher(num);
			return m.matches();
		}
	}

	/**
	 * 判断组织结构代码证书
	 * 
	 * @param oCode
	 * @return
	 */
	public static boolean matchOCode(String oCode) {
		if (oCode == null || "".equals(oCode)) {
			return false;
		} else {
			Pattern p = Pattern.compile(OCodeRex);
			Matcher m = p.matcher(oCode);
			return m.find();
		}
	}

	/**
	 * 判断是否存在浮点数
	 * 
	 * @param Double
	 * @return
	 */
	public static boolean matchDouble(String Double) {
		if (Double == null || "".equals(Double)) {
			return false;
		} else {
			Pattern p = Pattern.compile(doubleRex);
			Matcher m = p.matcher(Double);
			return m.find();
		}
	}

	/**
	 * 判断是否存在*号
	 * 
	 * @param Star
	 * @return
	 */
	public static boolean matchStar(String Star) {
		if (Star == null || "".equals(Star)) {
			return false;
		} else {
			Pattern p = Pattern.compile(starRex);
			Matcher m = p.matcher(Star);
			return m.find();
		}
	}

	/**
	 * 判断是否存在数字
	 * 
	 * @param Digital
	 * @return
	 */
	public static boolean matchDigital(String Digital) {
		if (Digital == null || "".equals(Digital)) {
			return false;
		} else {
			Pattern p = Pattern.compile(digitalRex);
			Matcher m = p.matcher(Digital);
			return m.find();
		}
	}

	/**
	 * 判断是否存在英文
	 * 
	 * @param english
	 * @return
	 */
	public static boolean matchEnglish(String english) {
		if (english == null || "".equals(english)) {
			return false;
		} else {
			Pattern p = Pattern.compile(englishRex);
			Matcher m = p.matcher(english);
			return m.find();
		}
	}

	/**
	 * 将一个map的所有value中的空格去除
	 * 
	 * @param map
	 * @return
	 */
	public static Map<String, Object> replaceSpace(Map<String, Object> map) {
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			if ("insertTime".equals(entry.getKey()) || "updateTime".equals(entry.getKey()))
				continue;
			entry.setValue((String.valueOf(entry.getValue())).replaceAll("\\s", "").replaceAll("", ""));
		}
		return map;
	}

	/**
	 * 判断是否为全角,全角返回true
	 * 
	 * @param param
	 * @return
	 */
	public static boolean isAllHalf(String param) {
		char[] chs = param.toCharArray();
		for (int i = 0; i < chs.length; i++) {
			if (!(('\uFF61' <= chs[i]) && (chs[i] <= '\uFF9F')) && !(('\u0020' <= chs[i]) && (chs[i] <= '\u007E'))) {
				return true;
			}
		}
		return false;
	}
	public static boolean isAllHalf(Object param) {
		return isAllHalf(String.valueOf(param));
	}

	/**
	 * 全角转半角
	 * 
	 * @param input
	 *            String.
	 * @return 半角字符串
	 */
	public static String ToDBC(String input) {
		char c[] = input.toCharArray();
		for (int i = 0; i < c.length; i++) {
			if (c[i] == '\u3000') {
				c[i] = ' ';
			} else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
				c[i] = (char) (c[i] - 65248);
			}
		}
		return new String(c);
	}
	public static String ToDBC(Object input) {
		return ToDBC(String.valueOf(input));
	}
	
	/**
	 * 判断Map里面的所有值,如果是全角则转换成半角
	 * @param map
	 * @return
	 */
	public static Map<String,Object> replaceMap(Map<String,Object> map){
		for(Entry<String, Object> entry : map.entrySet()){
			if(isAllHalf(entry.getValue()))
				map.put(entry.getKey(), ToDBC(entry.getValue()));
		}
		return map;
	}
}
