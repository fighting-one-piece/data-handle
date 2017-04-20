package org.platform.modules.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class ExtractDate extends GenericUDF {

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		for (int i = 0, len = arguments.length; i < len; i++) {
			String currentDate = String.valueOf(arguments[i].get());
			if (currentDate.indexOf("-") == -1 && (currentDate.length() != 4 || !isDigital(currentDate))) continue;
			return new Text(currentDate.split("-")[0]);
		}
		return null;
	}

	@Override
	public String getDisplayString(String[] children) {
		return null != children && children.length > 0 ? children[0] : null;
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
