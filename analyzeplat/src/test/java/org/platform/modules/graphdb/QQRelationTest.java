package org.platform.modules.graphdb;

import java.io.FileNotFoundException;
import java.util.List;

import org.platform.utils.QQRelationUtils;
import org.platform.utils.file.DefaultLineHandler;
import org.platform.utils.file.FileUtils;

public class QQRelationTest {
	
	public static void insertQQNode() {
		try {
			List<String> lines = FileUtils.readFromAbsolute("F:\\document\\doc\\201704\\qqdata", new DefaultLineHandler());
			for (String line : lines) {
				QQRelationUtils.insertQQNode(line);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void insertQunNode() {
		try {
			List<String> lines = FileUtils.readFromAbsolute("F:\\document\\doc\\201704\\qqqundata", new DefaultLineHandler());
			for (String line : lines) {
				QQRelationUtils.insertQunNode(line);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		insertQQNode();
	}
	
}
