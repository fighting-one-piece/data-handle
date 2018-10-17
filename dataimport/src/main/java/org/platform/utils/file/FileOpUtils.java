package org.platform.utils.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileOpUtils {
	
	public static List<String> readAllFiles(String path) {
		List<String> files = new ArrayList<String>();
		readAllFiles(path, files);
		return files;
	}
	
	public static void readAllFiles(String path, List<String> files) {
		File file = new File(path);
		if (file.isDirectory()) {
			File[] fileList = file.listFiles();
			if (null == fileList) return;
			for (int i = 0, len = fileList.length; i < len; i++) {
				readAllFiles(fileList[i].getAbsolutePath(), files);
			}
		} else if (file.isFile()) {
			files.add(file.getAbsolutePath());
		}
	}
	
}
