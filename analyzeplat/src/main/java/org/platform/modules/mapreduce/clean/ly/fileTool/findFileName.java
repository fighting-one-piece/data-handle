package org.platform.modules.mapreduce.clean.ly.fileTool;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.platform.modules.mapreduce.clean.util.HDFSUtils;

public class findFileName {
	public static void main(String[] args) throws IOException {
		BufferedWriter bw =new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\表名.txt")));
		List<String> filePathList = new ArrayList<String>();
		HDFSUtils.readAllFiles("hdfs://192.168.0.10:9000/warehouse_data/financial/logistics", null, filePathList);
		for(int i=0;i<filePathList.size();i++){
			System.out.println(filePathList.get(i));
			bw.write(filePathList.get(i).toString());
			bw.newLine();
		}
		System.out.println("写入完毕");
		bw.close();
	}
}	
