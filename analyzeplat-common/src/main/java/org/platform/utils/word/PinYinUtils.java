package org.platform.utils.word;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinYinUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(PinYinUtils.class);

	public static String getPinYinFromHanYu(String hanyu) {
		if (null == hanyu || hanyu.length() == 0) return null;
		HanyuPinyinOutputFormat outputFormat = new HanyuPinyinOutputFormat();
		outputFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
		outputFormat.setVCharType(HanyuPinyinVCharType.WITH_U_UNICODE);
		StringBuilder sb = new StringBuilder();
		try {
			for (int i = 0, len = hanyu.length(); i < len; i++) {
				String[] pinyins = PinyinHelper.toHanyuPinyinStringArray(hanyu.charAt(i), outputFormat);
				if(null == pinyins) continue;
				sb.append(pinyins[0]);
			}
		} catch (BadHanyuPinyinOutputFormatCombination e) {
			LOG.error(e.getMessage(), e);
			return sb.toString();
		}
		return sb.toString();
	}
	
	public static void main(String[] args) {
		System.out.println(getPinYinFromHanYu("四川省成都市"));
	}
	
}
