package org.platform.modules.hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class GenericUDFSplit extends GenericUDF {
	
	private ObjectInspectorConverters.Converter[] converters = null;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function SPLIT(s, regexp) takes exactly 2 arguments.");
		}
		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
					PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}
		return ObjectInspectorFactory.getStandardListObjectInspector(
				PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 2);
		if (arguments[0].get() == null || arguments[1].get() == null) return null;
		Text src = (Text) converters[0].convert(arguments[0].get());
		Text regex = (Text) converters[1].convert(arguments[1].get());
		ArrayList<Text> result = new ArrayList<Text>();
		for (String str : src.toString().split(regex.toString())) {
			result.add(new Text(str));
		}
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return "split(" + children[0] + ", " + children[1] + ")";
	}

}
