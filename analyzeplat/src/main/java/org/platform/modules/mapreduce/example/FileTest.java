package org.platform.modules.mapreduce.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileTest {

	public static void method1() throws Exception {
		Configuration conf = new Configuration();
		String path = "hdfs://192.168.0.10:9000/warehouse_data/trip/airplane/airplane__6130f11a_0bc1_417e_b2b8_dcd0cfacf52e";
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FSDataInputStream in = fs.open(new Path(path));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while (null != (line = br.readLine())) {
			System.out.println(line);
			System.out.println(in.getPos());
			System.out.println(in.available());
//			System.out.println(in.readUTF());
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
	
	public static void main(String[] args) throws Exception {
		method3();
	}
	
}
