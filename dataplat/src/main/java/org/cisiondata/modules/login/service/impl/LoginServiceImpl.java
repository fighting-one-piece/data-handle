package org.cisiondata.modules.login.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.login.dao.UserDAO;
import org.cisiondata.modules.login.entity.User;
import org.cisiondata.modules.login.service.ILoginService;
import org.cisiondata.utils.exception.BusinessException;
import org.cisiondata.utils.redis.RedisClusterUtils;
import org.springframework.stereotype.Service;

@Service("loginService")
public class LoginServiceImpl implements ILoginService {

	@Resource(name = "userDAO")
	private UserDAO userDAO = null;

	@Override
	public String Login(User user) throws BusinessException {
		String account = user.getAccount();
		String password = user.getPassword();
		if (StringUtils.isBlank(account) || StringUtils.isBlank(password))
			throw new BusinessException(ResultCode.PARAM_NULL);
		Map<String,Object> params = new HashMap<String, Object>();
		params.put("account", account);
		params.put("deleteFlag", false);
		User u = userDAO.readDataByCondition(params);
		if (null == u || null == u.getId())
			throw new BusinessException(ResultCode.ACCOUNT_NOT_EXIST);
		if (!password.equals(u.getPassword()))
			throw new BusinessException(ResultCode.ACCOUNT_PASSWORD_NOT_MATCH);
		
		String uuid = UUID.randomUUID().toString();
		RedisClusterUtils.getInstance().set(uuid, u,36000);
		return uuid;
	}
}
