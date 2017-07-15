package org.cisiondata.modules.web;

import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.login.entity.User;
import org.cisiondata.utils.exception.BusinessException;
import org.cisiondata.utils.redis.RedisClusterUtils;

public class WebUtils {

	public static User getCurrentUser(){
		WebContext webContext = WebContext.get();
		User user = null;
		if (null != webContext && null != webContext.getRequest()) {
			Object object = RedisClusterUtils.getInstance().get(webContext.getRequest().getHeader("accessToken"));
			if (null == object) object = RedisClusterUtils.getInstance().get(webContext.getRequest().getHeader("accesstoken"));
			if (null != object) user = (User)object;
			if (null == object) throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
		}
		return user;
	}
	
	public static String getCurrentAccout() {
		User user = getCurrentUser();
		return null == user ? null : user.getAccount();
	}
	public static String getDirectory() {
		User user = getCurrentUser();
		return null == user ? null : user.getDirectory();
	}
}
