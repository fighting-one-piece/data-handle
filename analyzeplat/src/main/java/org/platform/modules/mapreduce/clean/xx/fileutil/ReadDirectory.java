package org.platform.modules.mapreduce.clean.xx.fileutil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 读取系统目录并生成树形目录写入指定文件
 * 
 * @author lixin
 */
public class ReadDirectory {

	// 文件所在的层数
	private int fileLevel;

	/**
	 * 生成输出格式
	 * 
	 * @param name
	 *            输出的文件名或目录名
	 * @param level
	 *            输出的文件名或者目录名所在的层次
	 * @return 输出的字符串
	 */
	public String createPrintStr(String name, int level) {
		// 输出的前缀
		String printStr = "";
		// 按层次进行缩进
		/*for (int i = 0; i < level; i++) {
			printStr = printStr;
		}*/
		printStr = printStr + name;
		System.out.println(printStr);
		return printStr;
	}

	/**
	 * 输出初始给定的目录
	 * 
	 * @param dirPath
	 *            给定的目录
	 */
	public void printDir(String dirPath) {

		// 将给定的目录进行分割
		String[] dirNameList = dirPath.split("\\\\");

		fileLevel = dirNameList.length;
		// 按格式输出
		for (int i = 0; i < dirNameList.length; i++) {

		}
	}

	/**
	 * 输出给定目录下的文件，包括子目录中的文件
	 * 
	 * @param dirPath
	 * @param bw
	 * @throws IOException
	 */
	public void readFile(String dirPath, BufferedWriter bw, BufferedWriter bk, String keyWord) throws IOException {
		String regKeyWord = "^(" + keyWord + ")+.*$";
		// 建立当前目录中文件的File对象
		File file = new File(dirPath);
		// 取得代表目录中所有文件的File对象数组
		File[] list = file.listFiles();
		// 遍历file数组
		if (list != null) {
			for (int i = 0; i < list.length; i++) {
				try {
					if (list[i].isDirectory()) {
						String mu = createPrintStr(list[i].getName(), fileLevel);
						bw.write(mu);
						bw.newLine();
						fileLevel++;
						// 递归子目录
						readFile(list[i].getPath(), bw, bk, keyWord);
						fileLevel--;
					} else {
						if (list[i].getName().matches(regKeyWord)) {
							String l = createPrintStr(list[i].getName(), fileLevel);
							bk.write(l);
							bk.newLine();
						} else {
							String l = createPrintStr(list[i].getName(), fileLevel);
							bw.write(l);
							bw.newLine();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 获取文件大小
	 * 
	 * @param f
	 * @return
	 */
	/*private static int getFileSize(File f) {
		InputStream fis = null;
		try {
			fis = new FileInputStream(f);
			return fis.available();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}*/

	/**
	 * 依次给如参数 args[0] 输入路径 args[1] 输出路径 args[2]关键字检索输出路径 args[3]关键字
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			// 保存目录
			FileWriter fw = new FileWriter("C:\\Users\\Administrator\\Desktop\\2\\1.txt");
			BufferedWriter bw = new BufferedWriter(fw);
			// 关键字检索目录
			FileWriter fk = new FileWriter("C:\\Users\\Administrator\\Desktop\\2\\1.txt");
			BufferedWriter bk = new BufferedWriter(fk);

			String keyWord = "";
			ReadDirectory rd = new ReadDirectory();
			// 查看目录
			String dirPath = "C:\\Users\\Administrator\\Desktop\\1";
			rd.printDir(dirPath);
			rd.readFile(dirPath, bw, bk, keyWord);
			bw.close();
			bk.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
