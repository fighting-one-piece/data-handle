package org.cisiondata.modules.dataimport.service;




public interface IUploadAndToMysql {
	
	/**
	 * 执行脚本文件，将文件导入MySQL
	 * @param fileName 要导入的文件名   
	 * @param filesPath 文件路径
	 * @param person  导入人
	 * @param separator  分割符
	 * @return
	 */
	public void fileToMySQL(String fileName ,String filesPath,String person,String separator);
	
	
	/**
	 * 在本地tomcat上创建文件夹
	 * @param destDirName 文件的绝对路径
	 * @return
	 */
	public boolean createDir(String destDirName);
	
	/**
	 * 删除本地tomcat上的文件
	 * @param file 路径
	 */
	public void deleteFile(String filePath);
	
}
