package org.cisiondata.modules.login.controller;

import javax.annotation.Resource;

import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.abstr.web.WebResult;
import org.cisiondata.modules.login.entity.User;
import org.cisiondata.modules.login.service.ILoginService;
import org.cisiondata.utils.exception.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class LoginController {

	private Logger LOG = LoggerFactory.getLogger(LoginController.class);
	
	@Resource(name = "loginService")
	private ILoginService loginService = null;
	
	@RequestMapping(value = "/login",method = RequestMethod.POST,headers = "Accept=application/json")
	@ResponseBody
	public WebResult login(@RequestBody User user){
		WebResult result = new WebResult();
		try {
			result.setData(loginService.Login(user));
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch (BusinessException bu) {
			LOG.error(bu.getDefaultMessage(),bu);
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}
}
