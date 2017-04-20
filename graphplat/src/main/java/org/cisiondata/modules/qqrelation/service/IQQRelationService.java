package org.cisiondata.modules.qqrelation.service;

import java.util.List;

import org.cisiondata.utils.exception.BusinessException;

public interface IQQRelationService {
	
	/**
	 * 批量插入QQ号
	 * @param nodes
	 * @throws BusinessException
	 */
	public void insertQQNodes(List<String> nodes) throws BusinessException;
	
	/**
	 * 批量插入QQ群
	 * @param nodes
	 * @throws BusinessException
	 */
	public void insertQQQunNodes(List<String> nodes) throws BusinessException;

}
