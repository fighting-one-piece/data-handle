package org.cisiondata.modules.dataimport.controller;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.abstr.web.WebResult;
import org.cisiondata.modules.dataimport.service.IMYSQL2HDFSService;
import org.cisiondata.utils.exception.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MYSQL2HDFSController {
	Logger LOG = LoggerFactory.getLogger(MYSQL2HDFSController.class);
	
	@Resource(name = "mysql2hdfsService")
	private IMYSQL2HDFSService mysql2hdfsService = null;

	@RequestMapping(value = "/createjson", method = RequestMethod.POST)
	@ResponseBody
	public WebResult createJson(String database,String table,String index ,String type,String[] mysqlColumn,String[] hdfsColumn) {
		WebResult result = new WebResult();
		try {
			result.setData(mysql2hdfsService.createJson(database, table, index, type, mysqlColumn, hdfsColumn));
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch(BusinessException bu){
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e) {
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}

	@ResponseBody
	@RequestMapping(value = "/execute", produces = "application/json; charset=utf-8")
	public Map<String, Object> executeOneJson(String jsonname) {
		try {
			String path = "/home/ym/Datax/datax/job/" + jsonname;
			String msg = mysql2hdfsService.executeOneJson(path);
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("result", msg);
			return map;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@RequestMapping(value = "/jsonlist",method=RequestMethod.GET)
	@ResponseBody
	public WebResult getJsonList() {
		WebResult result = new WebResult();
		try {
			result.setData(mysql2hdfsService.readJsonList());
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch(BusinessException bu){
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e) {
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}

	@RequestMapping(value = "/readJson",method = RequestMethod.GET)
	@ResponseBody
	public WebResult readJson(String jsonName) {
		WebResult result = new WebResult();
		try {
			result.setData(mysql2hdfsService.readJson(jsonName));
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch(BusinessException bu){
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e) {
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}
	
	@RequestMapping(value = "/updateJson",method = RequestMethod.POST)
	@ResponseBody
	public WebResult updateJson(String jsonName,String data) {
		WebResult result = new WebResult();
		try {
			mysql2hdfsService.updateJson(jsonName, data);
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch(BusinessException bu){
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e) {
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}
	@RequestMapping(value = "/deleteJson",method = RequestMethod.POST)
	@ResponseBody
	public WebResult deleteJson(String jsonName) {
		WebResult result = new WebResult();
		try {
			mysql2hdfsService.deleteJson(jsonName);
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch(BusinessException bu){
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e) {
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}

}
