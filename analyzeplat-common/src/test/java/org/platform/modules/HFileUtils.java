package org.platform.modules;

import java.util.ArrayList;
import java.util.List;

import org.platform.utils.file.DefaultLineHandler;
import org.platform.utils.file.HDFSUtils;

public class HFileUtils {
	
	public static void main(String[] args) {
		List<String> result = new ArrayList<String>();
		result.add("aa1$#$aa2$#$aa3$#$aa4$#$aa5");
		result.add("bb1$#$bb2$#$bb3$#$bb4$#$bb5");
		result.add("cc1$#$cc2$#$cc3$#$cc4$#$cc5");
		HDFSUtils.write("hdfs://192.168.0.124:9000/user/ljp/records-2-m-00000", result);
		
		List<String> lines = HDFSUtils.readFile(
				"hdfs://192.168.0.124:9000/user/ljp/records-2-m-00000", new DefaultLineHandler());
		lines.forEach(line -> System.err.println(line));
	}
	
}
