package org.platform.modules.mapreduce.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.recognition.PersonName;

public class FileTest {

	public static void method1() throws Exception {
		Configuration conf = new Configuration();
		String path = "hdfs://192.168.0.10:9000/warehouse_data/work/resume/resume__0a1438ea_20cc_4147_8b05_db06f58847df";
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FSDataInputStream in = fs.open(new Path(path));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while (null != (line = br.readLine())) {
			System.out.println(line);
			System.out.println(in.getPos());
			System.out.println(in.available());
			System.out.println(in.readUTF());
		}
		in.close();
		br.close();
	}
	
	public static void method2() throws Exception {
		RandomAccessFile rf = new RandomAccessFile("", "rw");
		System.out.println(rf.readLine());
		rf.close();
	}
	
	public static void method3() throws Exception {
		String path = "F:\\document\\doc\\201703\\test.txt";
		InputStream in = new FileInputStream(new File(path));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		String metadadaString = "﻿address$#$name$#$phone$#$rcall$#$cancelOrder$#$endTime$#$sourceFile$#$updateTime$#$inputPerson";
		String[] metadatas = metadadaString.split("\\$#\\$");
		while (null != (line = br.readLine())) {
			Map<String, Object> result = new HashMap<String, Object>();
			String[] fields = line.split("\\$#\\$");
			System.out.println(fields.length);
			for (int i = 0, len = metadatas.length; i < len; i++) {
				result.put(metadatas[i], fields[i]);
			}
			System.out.println(result);
		}
		br.close();
		in.close();
	}
	
	public static void v1() {
		System.out.println(PersonName.recognize(WordSegmenter.seg("@李建鹏")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("*何泽杰")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("！宋康明")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("李*欣")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("鹏得龙")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("刘（宇）TB")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("谢[鑫]无耻")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("谭佳龙VS王仕恩")));
		System.out.println(PersonName.recognize(WordSegmenter.seg("诸葛正我厉害")));
	}
	
	public static void v2() throws IOException {
		Configuration conf = new Configuration();
		String warehousePath = "hdfs://192.168.0.10:9000/warehouse_data/";
		FileSystem fs = FileSystem.get(URI.create(warehousePath), conf);
		Path dir = new Path(warehousePath);
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(dir, true);
		long fileTotalLines = 0L;
		while(iterator.hasNext()) {
			LocatedFileStatus fileStatus = iterator.next();
			if (fileStatus.isFile()) {
				long fileLines = v3(fs, fileStatus.getPath());
				System.out.println(fileLines + "  " + fileStatus.getPath());
				fileTotalLines = fileTotalLines + fileLines;
			}
		}
		System.out.println("fileTotalLines: " + fileTotalLines);
	}
	
	@SuppressWarnings("unused")
	public static long v3(FileSystem fs, Path path) throws IOException {
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		long count = 0;
		while (null != (line = br.readLine())) {
			count++;
		}
		in.close();
		br.close();
		return count;
	}
	
	public static void main(String[] args) throws Exception {
		v2();
	}
	
}
