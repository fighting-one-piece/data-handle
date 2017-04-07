package org.platform.modules.mapreduce.clean.xx.util;

import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class TestReg {
	public static void main(String[] args) {
		String str = " *^*&^*";
		System.out.println((CleanUtil.matchNum(str)||(!CleanUtil.matchEnglish(str)&&!CleanUtil.matchChinese(str))));
	}
}
