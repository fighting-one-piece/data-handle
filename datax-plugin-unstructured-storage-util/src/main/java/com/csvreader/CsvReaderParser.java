package com.csvreader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CsvReaderParser {

	public static void main(String[] args) throws Exception {
		InputStream in = new FileInputStream(new File("F:\\a.txt"));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		CCsvReader csvReader = new CCsvReader(br, "|!#", 3);
		while (csvReader.readRecord()) {
			System.err.println("column count: " + csvReader.getColumnCount());
			String[] values = csvReader.getValues();
			for (String value : values) {
				System.err.println(value);
			}
			System.err.println("current record: " + csvReader.getCurrentRecord());
			System.err.println("current raw: " + csvReader.getRawRecord());
		}
		br.close();
		csvReader.close();
	}
	
}
