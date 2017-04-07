package org.platform.modules.mapreduce.clean.ly.fileTool;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.platform.modules.mapreduce.clean.util.HDFSUtils;



public class StatisticsFileSize {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			//指定路径
			String path = "hdfs://192.168.0.10:9000/warehouse_data/work/resume";
//			String path ="hdfs://192.168.0.115:9000/elasticsearch/account";
			//指定正则表达式
			String regex = "^(resume)+.*$";
			FileSystem fs = FileSystem.get(URI.create(path),conf);
			List<String> list = new ArrayList<String>();
			HDFSUtils.readAllFiles(path, regex, list);
			System.out.println("符合条件的文件数： "+list.size()+"个");
			long size = 0;
			for(String str :list){
				size =fs.getFileStatus(new Path(str)).getLen()+size;
			}
			System.out.println("符合条件的文件总大小为：  "+getPrintSize(size));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static String getPrintSize(long size) { 
	    //如果字节数少于1024，则直接以B为单位，否则先除于1024，后3位因太少无意义  
	    if (size < 1024) {  
	        return String.valueOf(size) + "B";  
	    } else {  
	        size = size / 1024; 
	    }  
	    //如果原字节数除于1024之后，少于1024，则可以直接以KB作为单位  
	    //因为还没有到达要使用另一个单位的时候  
	    //接下去以此类推  
	    if (size < 1024) {  
	        return String.valueOf(size) + "KB";  
	    } else {  
	        size = size / 1024;  
	    }  
	    if (size < 1024) {  
	        //因为如果以MB为单位的话，要保留最后1位小数，  
	        //因此，把此数乘以100之后再取余  
	        size = size * 100;  
	        return String.valueOf((size / 100)) + "."  
	                + String.valueOf((size % 100)) + "MB";  
	    } else {  
	        //否则如果要以GB为单位的，先除于1024再作同样的处理  
	        size = size * 100 / 1024;  
	        return String.valueOf((size / 100)) + "."  
	                + String.valueOf((size % 100)) + "GB";  
	    }  
	}  
}