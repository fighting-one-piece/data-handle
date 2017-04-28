package org.platform.modules.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.platform.utils.AddressUtils;

public class ExtractProvinceCity extends GenericUDF {
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		return ObjectInspectorFactory.getStandardListObjectInspector(
				PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments.length != 1) return null;
		String address = String.valueOf(arguments[0].get());
		List<String> ads = AddressUtils.extract3ADFromAddress(address);
		List<Text> result = new ArrayList<Text>();
		for (int i = 0, len = ads.size(); i < len; i++) {
			result.add(new Text(ads.get(i)));
		}
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		return null != children && children.length > 0 ? children[0] : null;
	}
}
