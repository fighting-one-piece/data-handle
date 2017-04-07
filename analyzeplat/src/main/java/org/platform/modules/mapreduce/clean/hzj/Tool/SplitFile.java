package org.platform.modules.mapreduce.clean.hzj.Tool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SplitFile {
	public static void split(String src,int fileSize,String dest){
		
		if("".equals(src)||src==null||fileSize==0||"".equals(dest)||dest==null){
			System.out.println("分割失败");
		}
		
		File srcFile = new File(src);//源文件
		
		long srcSize = srcFile.length();//源文件的大小
		long destSize = 1024*20480*fileSize;//目标文件的大小（分割后每个文件的大小）
		
		int number = (int)(srcSize/destSize);
		number = srcSize%destSize==0?number:number+1;//分割后文件的数目
		
		String fileName = src.substring(src.lastIndexOf("\\"));//源文件名
		
		InputStream in = null;//输入字节流
		BufferedInputStream bis = null;//输入缓冲流
		byte[] bytes = new byte[1024*1024];//每次读取文件的大小为1MB
		int len = -1;//每次读取的长度值
		try {
			in = new FileInputStream(srcFile);
			bis = new BufferedInputStream(in);
			for(int i=0;i<number;i++){
				/*截取文件名*/
			    String a = fileName.substring(0,4);
				String destName = dest+File.separator+a+"_"+i+".txt";
				OutputStream out = new FileOutputStream(destName);
				BufferedOutputStream bos = new BufferedOutputStream(out);
				int count = 0;
				while((len = bis.read(bytes))!=-1){
					bos.write(bytes, 0, len);//把字节数据写入目标文件中
					count+=len;
					if(count>=destSize){
						break;
					}
				}
				bos.flush();//刷新
				bos.close();
				out.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			//关闭流
			try {
				if(bis!=null)bis.close();
				if(in!=null)in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		/**
		 * 分割测试
		 */
		/*文件读取路径*/
		String src = "C:\\Users\\meishu1\\Desktop\\xin2.txt";//要分割的大文件
		int fileSize = 10;
		/*String src = null;
		BufferedReader br = null;
		InputStreamReader InputStream = null;
		try {
			InputStream = new InputStreamReader(new FileInputStream(file));
			br = new BufferedReader(InputStream);
			src = br.readLine();
			替换文件的分隔符为 ","
			src = file.replaceAll("----",",");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */   
		/*文件输出路径*/
		String dest = "C:\\Users\\meishu1\\Desktop\\222\\";//文件分割后保存的路径
		System.out.println("分割开始...");
		split(src, fileSize, dest);
		System.out.println("分割完成");
		
		/**
		 * 合并测试
		 *//*
		//合并后文件的保存路径
		String destPath = "D:\\upan";
		//要合并的文件路径
		String[] srcPaths = {
				"D:\\JDK_API_1_6_zh_CN.CHM-0.dat",
				"D:\\JDK_API_1_6_zh_CN.CHM-1.dat",
				"D:\\JDK_API_1_6_zh_CN.CHM-2.dat",
				"D:\\JDK_API_1_6_zh_CN.CHM-3.dat"};
		System.out.println("开始合并。。。");
		merge(destPath, srcPaths);
		System.out.println("合并结束");*/
			
	}
	
}
