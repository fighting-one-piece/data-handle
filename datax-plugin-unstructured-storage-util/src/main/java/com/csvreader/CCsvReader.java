package com.csvreader;

import java.io.IOException;
import java.io.Reader;

import org.apache.commons.lang3.StringUtils;

public class CCsvReader extends CsvReader {
	
	private char[] delimiters = null;
	
	private int columnCount = 0;
	
	public CCsvReader(Reader reader, String fieldDelimiter, int columnCount) {
		super(reader, StringUtils.isBlank(fieldDelimiter) ? ',' : fieldDelimiter.charAt(0));
		this.delimiters = fieldDelimiter.toCharArray();
		this.columnCount = columnCount;
	}
	
	@Override
	public String[] getValues() throws IOException {
		String[] values = super.getValues();
		if (null != values && values.length == columnCount) {
			return values;
		}
		String rawRecord = getRawRecord();
		for (int i = 1, len = delimiters.length; i < len; i++) {
			values = rawRecord.split(String.valueOf(delimiters[i]));
			if (null != values && values.length == columnCount) {
				return values;
			}
		}
		return values;
	}

	public int getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}
	
	
	
}