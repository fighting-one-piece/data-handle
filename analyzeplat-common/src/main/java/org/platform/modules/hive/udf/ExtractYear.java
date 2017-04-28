package org.platform.modules.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ExtractYear extends UDF {

	public String evaluate(String arg) {
		return (arg.indexOf("-") == -1 && (arg.length() != 4 || !isDigital(arg))) ? null : arg.split("-")[0];
	}
	
	/**
	 * 数字验证
	 * @param str
	 * @return
	 */
	private boolean isDigital(String str) {
		return str.matches("^[0-9]*$");
	}
	
}
