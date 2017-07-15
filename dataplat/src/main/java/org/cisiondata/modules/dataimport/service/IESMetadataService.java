package org.cisiondata.modules.dataimport.service;

import java.util.List;
import java.util.Set;

import org.cisiondata.utils.exception.BusinessException;

public interface IESMetadataService {

	/**
	 * 读取所有index
	 * @return
	 * @throws BusinessException
	 */
	public Set<String> readIndices() throws BusinessException;
	
	/**
	 * 根据index读取types
	 * @param index
	 * @return
	 * @throws BusinessException
	 */
	public List<String> readIndexTypes(String index) throws BusinessException;
	
	/**
	 * 根据index，type获取属性列表
	 * @param index
	 * @param type
	 * @return
	 * @throws BusinessException
	 */
	public List<String> readIndexTypeAttributes(String index,String type) throws BusinessException;
}
