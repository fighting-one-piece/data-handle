package org.platform.modules.mapreduce.clean.lx.function.function;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 *在hadoop上移动文件，并用UUID重命名文件 
 * @author Administrator
 *
 */

public class moveFile {
	//存放目录下的所有满足条件的绝对路径
	public static List<String> list = new ArrayList<String>();
	
	public static void main(String[] args){
		String src = "hdfs://192.168.0.10:9000/warehouse_data/utils/telecom";
		String dst = "hdfs://192.168.0.10:9000/warehouse_data/utils/";
		/**
		 *  src	需要移动的文件或目录
		 * dst	移动的目标目录
		 * param		指定移动文件类型（eg:account   email）
		 */
		MoveFileAndReName(src,dst,"telecom");
	}
	
	/**
	 * 移动HDFS本地文件到指定目录，并用uuid命名
	 * @param src	需要移动的文件或目录
	 * @param dst	移动的目标目录
	 * @param param		指定移动文件类型（eg:account   email）
	 */
	
	public static void MoveFileAndReName(String src, String dst ,String param){
		Configuration conf = new Configuration();
        try {
			FileSystem fs = FileSystem.get(URI.create(src),conf);
			//递归查找指定目录下的文件（满足正则表达式的文件）
			findFiles(fs,new Path(src));
			if(list.size()<=0){
				System.out.println("未查找到可用文件");
				System.exit(2);
			}
			if(!fs.isDirectory(new Path(dst))){
				System.out.println("输出目录不存在，请确认目标目录是否存在");
				System.exit(2);
			}
			for(String str :list){
				System.out.println(str);
			}
			System.out.println("请确认你要移动文件的文件，并输入数字1确认（不可恢复）");
			System.out.println();
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(System.in);
			if(scan.hasNextLine()){
			for(int i=0;i<list.size();i++){
					UUID uuid = UUID.randomUUID();
					String localFileName=list.get(i).substring(list.get(i).lastIndexOf("/")+1);
					System.out.println(localFileName+" ————>>  "+dst+"/"+param+"__"+uuid.toString().replaceAll("-", "_"));
					fs.rename(new Path(list.get(i)), new Path(dst+"/"+param+"__"+uuid.toString().replaceAll("-", "_")));
			}
			System.out.println("移动文件成功");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//递归查找满足正则表达式的文件
	public static List<String> findFiles(FileSystem hdfs, Path path) {
		String regex1 = "^(000)+.*$";
		String regex2 = "^(	000)+.*$";
		try {
			if (hdfs == null || path == null) {
				return null;
			}
			// 获取文件列表
			FileStatus[] files = hdfs.listStatus(path);
			// 展示文件信息
			for (int i = 0; i < files.length; i++) {
				try {
					if (files[i].isDirectory()) {
						// 递归调用
						findFiles(hdfs, files[i].getPath());
					} else if (files[i].isFile()) {
						String name = files[i].getPath().getName();
						if (name.matches(regex1) || name.matches(regex2)) {
							list.add(files[i].getPath().toString());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
}
