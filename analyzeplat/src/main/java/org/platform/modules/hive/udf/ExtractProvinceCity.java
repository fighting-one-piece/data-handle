package org.platform.modules.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;
import org.platform.utils.AdministrativeDivisionUtils;

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
		List<Text> result = new ArrayList<Text>();
		if (StringUtils.isNotBlank(address)) {
			List<Word> words = WordSegmenter.seg(address, SegmentationAlgorithm.MaximumMatching);
			AdministrativeDivisionMatcher adMatcher = new AdministrativeDivisionMatcher();
			for (int i = 0, len = words.size(); i < len; i++) {
				String word = words.get(i).getText();
				if (AdministrativeDivisionUtils.getProvinces().contains(word)) {
					adMatcher.provinceMatch();
					if (AdministrativeDivisionUtils.getCityProvinceMap().containsKey(word)) {
						word = AdministrativeDivisionUtils.getCityProvinceMap().get(word);
					}
					result.add(new Text(word));
				}
				if (AdministrativeDivisionUtils.getCities().contains(word)) {
					adMatcher.cityMatch();
					if (AdministrativeDivisionUtils.getCityMap().containsKey(word)) {
						word = AdministrativeDivisionUtils.getCityMap().get(word);
					}
					if (adMatcher.getProvinceMatch() == 0) {
						result.add(new Text(AdministrativeDivisionUtils.getCityProvinceMap().get(word)));
					}
					result.add(new Text(word));
				}
			}
		}
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		return null != children && children.length > 0 ? children[0] : null;
	}
}
