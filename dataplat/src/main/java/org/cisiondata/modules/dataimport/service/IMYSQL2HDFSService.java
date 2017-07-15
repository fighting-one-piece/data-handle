package org.cisiondata.modules.dataimport.service;

import java.util.List;
import java.util.Map;

import org.cisiondata.utils.exception.BusinessException;

/**
 * @author xiexin
 *
 */
public interface IMYSQL2HDFSService {
	/**
	 * 根据模板创建json文件
	 * @param request
	 * @param response
	 * @return 执行结果
	 */
	public String createJson(String database,String table,String index ,String type,String[] mysqlColumn,String[] hdfsColumn) throws BusinessException;
	
	/**
	 * 执行单个json文件
	 * @param path json文件路径
	 * @return 执行结果
	 */
	public String executeOneJson(String path);
	
	/**
	 * 获取所有json文件列表
	 * @param path json文件夹路径
	 * @return json文件列表
	 */
	public List<Map<String,Object>> readJsonList() throws BusinessException;
	
	/**
	 * 读取Json文件
	 * @param fileName json文件名
	 * @return 读取结果
	 */
	public String readJson(String fileName) throws BusinessException;
	
	/**
	 * 修改json文件
	 * @param fileName
	 * @throws BusinessException
	 */
	public void updateJson(String fileName,String data) throws BusinessException;
	
	/**
	 * 删除json文件
	 * @param fileName
	 * @throws BusinessException
	 */
	public void deleteJson(String fileName) throws BusinessException;
	
}