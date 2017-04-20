package org.platform.modules.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.platform.utils.judge.IdCardUtils;

public class ExtractIdCard extends UDF {

	public String evaluate(String idCard) {
		if (!IdCardUtils.isValidIdCard(idCard)) return null;
		return idCard.length() == 15 ? IdCardUtils.convertIdCardFrom15To18(idCard) : idCard;
	}
	
}
