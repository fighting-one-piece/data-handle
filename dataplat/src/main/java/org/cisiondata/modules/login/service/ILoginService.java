package org.cisiondata.modules.login.service;

import org.cisiondata.modules.login.entity.User;
import org.cisiondata.utils.exception.BusinessException;

public interface ILoginService {

	/**
	 * 登陆
	 * @param account
	 * @param password
	 * @return
	 * @throws BusinessException
	 */
	public String Login(User user) throws BusinessException;
}
