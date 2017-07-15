package org.cisiondata.modules.dataimport.service.impl;

import java.io.File;
import java.io.IOException;

import org.cisiondata.modules.dataimport.service.IUploadAndToMysql;
import org.springframework.stereotype.Service;

@Service("uploadAndToMysql")
public class UploadAndToMysqlImpl implements IUploadAndToMysql {

	@Override
	public boolean createDir(String destDirName) {
		File dir = new File(destDirName);
		if (dir.exists())
			return false;
		if (!destDirName.endsWith(File.separator)) { // 结尾是否以"/"结束
			destDirName = destDirName + File.separator;
		}
		dir.mkdir();
		return true;
	}

	@Override
	public void deleteFile(String filePath) {
		File file = new File(filePath);
		if (file.exists()) {// 判断文件是否存在
			if (file.isFile()) {// 判断是否是文件
				file.delete();// 删除文件
			} else if (file.isDirectory()) {// 否则如果它是一个目录
				File[] files = file.listFiles();// 声明目录下所有的文件 files[];
				for (int i = 0; i < files.length; i++) {// 遍历目录下所有的文件
					this.deleteFile(files[i].toString());// 把每个文件用这个方法进行迭代
				}
				file.delete();// 删除文件夹
			}
		} else {
			System.out.println("所删除的文件不存在");
		}
	}

	@Override
	public void fileToMySQL(String fileName, String filesPath , String person, String separator) {
		try {
			// linux服务器打包执行
			if (fileName.endsWith(".xls") || fileName.endsWith(".xlsx")) {
				String pypath = "/home/ym/FileImport/forPython.sh ";
				Process ps = Runtime.getRuntime().exec(pypath + filesPath+" "+person);
				ps.waitFor();
			} else if (fileName.endsWith(".txt")) {
				String shpath = "/home/ym/FileImport/txt2mysql.sh " +person+" "+ filesPath.substring(0,filesPath.lastIndexOf("/"))+" "+separator;
				Process ps = Runtime.getRuntime().exec(shpath);
				ps.waitFor();
			}
		} catch (IOException | InterruptedException e) {
			System.out.println("导入出错");
			e.printStackTrace();
		}
	}

}
