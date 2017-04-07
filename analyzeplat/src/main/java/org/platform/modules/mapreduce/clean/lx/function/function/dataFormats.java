package org.platform.modules.mapreduce.clean.lx.function.function;

import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * 修改long型号日期
 */
public class dataFormats{
	
	
	public static String dataFormat(String fileValue){
		SimpleDateFormat myFmt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuffer sBuffer = new StringBuffer();
		String file = "";
		for(String string:fileValue.split("[^\\d]")){
			sBuffer.append(string);
		}
		if(10<=sBuffer.length()&&sBuffer.length()<=13){
			switch (sBuffer.length()) {
			case 10:
				sBuffer = sBuffer.append("000");
				file =myFmt2.format(new Date(Long.parseLong(sBuffer.toString())));
				break;
			case 11:
				sBuffer = sBuffer.append("00");
				file =myFmt2.format(new Date(Long.parseLong(sBuffer.toString())));				
				break;
			case 12:
				sBuffer = sBuffer.append("0");
				file =myFmt2.format(new Date(Long.parseLong(sBuffer.toString())));				
				break;
			}	
		}else {		
			file=fileValue;
		}
		return file;
	}
	
	public static void main(String[] args) {
		System.out.println(dataFormat("1213432222"));
	}
}
