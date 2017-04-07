package org.platform.modules.mapreduce.clean.skm.HDFS2HDFS;

public class TestRegx {

	public static void main(String[] args) {

		String test = "12232";
		//String phoneRex = "1(3[0-9]|4[57]|5[0-35-9]|7[0135678]|8[0-9])\\d{8}";
//		String idCardRex = "\\d{17}(\\d|X|x)|\\d{15}";
//		String callRex = "0[0-9]{3}-?[2-9][0-9]{6}|0[0-9]{2}-?[2-9][0-9]{7}|400[0-9]{7}|[2-9][0-9]{6,7}";
//		String emailRex = "[\\w!#$%&'*+//=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+//=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?";
//		String IPRex = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))";
//		String QQRex = "[1-9][0-9]{4,9}";
		String red="\\d+(?:\\,\\d+)?(?:\\.\\d+)?";
		///String red="[1-9]\\d{5}(?!\\d)";
		if (test.matches(red)) {
			System.out.println("succesful");
		} else {
			System.out.println("error");
		}

	}

}
