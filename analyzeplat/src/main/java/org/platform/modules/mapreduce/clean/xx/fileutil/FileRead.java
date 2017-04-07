package org.platform.modules.mapreduce.clean.xx.fileutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * 匹配两个文件的表名，并对匹配的表名进行删除
 * 
 * @author xiexin
 *
 */
public class FileRead {

	public void ReadFile() {

	}

	/**
	 * 读取某个文件夹下的所有文件
	 */
	public static boolean readfile(String originalPath, String matchPath) throws FileNotFoundException, IOException {
		try {

			File file = new File(originalPath);
			if (!file.isDirectory()) {
				System.out.println("文件");
				System.out.println("path=" + file.getPath());
				System.out.println("absolutepath=" + file.getAbsolutePath());
				System.out.println("name=" + file.getName());

			} else if (file.isDirectory()) {
				System.out.println("文件夹");
				String[] filelist = file.list();
				String buff = readFile(matchPath);
				for (int i = 0; i < filelist.length; i++) {
					File readfile = new File(originalPath + "\\" + filelist[i]);
					if (!readfile.isDirectory()) {
						// System.out.println("path=" + readfile.getPath());
						// System.out.println("absolutepath=" +
						// readfile.getAbsolutePath());
						System.out.println(readfile.getName());
						if (buff.contains(readfile.getName())) {
							readfile.delete();
							System.out.println("删除 "+readfile.getName()+"成功！");
						}
					} else if (readfile.isDirectory()) {
						readfile(originalPath + "\\" + filelist[i], matchPath);
					}
				}

			}

		} catch (FileNotFoundException e) {
			System.out.println("readfile()   Exception:" + e.getMessage());
		}
		return true;
	}

	public static String readFile(String filePath) throws IOException {
		FileReader fileReader = new FileReader(new File(filePath));
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String str = "";
		String buff = "";
		while ((str = bufferedReader.readLine()) != null) {
			buff += str + ",";
		}
		bufferedReader.close();
		fileReader.close();
		return buff;
	}

	/**
	 * 删除某个文件夹下的所有文件夹和文件
	 */

	public static boolean deletefile(String delpath) throws FileNotFoundException, IOException {
		try {

			File file = new File(delpath);
			if (!file.isDirectory()) {
				System.out.println("1");
				file.delete();
			} else if (file.isDirectory()) {
				System.out.println("2");
				String[] filelist = file.list();
				for (int i = 0; i < filelist.length; i++) {
					File delfile = new File(delpath + "\\" + filelist[i]);
					if (!delfile.isDirectory()) {
						System.out.println("path=" + delfile.getPath());
						System.out.println("absolutepath=" + delfile.getAbsolutePath());
						System.out.println("name=" + delfile.getName());
						delfile.delete();
						System.out.println("删除文件成功");
					} else if (delfile.isDirectory()) {
						deletefile(delpath + "\\" + filelist[i]);
					}
				}
				file.delete();

			}

		} catch (FileNotFoundException e) {
			System.out.println("deletefile()   Exception:" + e.getMessage());
		}
		return true;
	}

	public static void main(String[] args) {
		try {
			readfile("C:\\Users\\Administrator\\Desktop\\1", "C:\\Users\\Administrator\\Desktop\\匹配结果");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("ok");
	}
}
