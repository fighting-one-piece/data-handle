package org.cisiondata.modules.qqrelation.controller;

import javax.annotation.Resource;

import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.abstr.web.WebResult;
import org.cisiondata.modules.qqrelation.service.IQQRelationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QQRelationController {
	
	private Logger LOG = LoggerFactory.getLogger(QQRelationController.class);

	@Resource(name = "qqRelationService")
	private IQQRelationService qqRelationService = null;
	
	@ResponseBody
	@RequestMapping(value = "/nodes", method = RequestMethod.GET)
	public String insertNode(String nodeJSON) {
		return "success";
	}
	
	@ResponseBody
	@RequestMapping(value = "/qqs", method = RequestMethod.POST)
	public String insertQQNode(String nodeJSON) {
		try {
			qqRelationService.insertQQNode(nodeJSON);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return e.getMessage();
		}
		return "success";
	}
	
	@ResponseBody
	@RequestMapping(value = "/qqs/{qqNum}", method = RequestMethod.GET)
	public WebResult readQQNode(@PathVariable("qqNum") String qqNum) {
		WebResult webResult = new WebResult();
		try {
			webResult.setData(qqRelationService.readQQNodeDataList(qqNum));
			webResult.setResultCode(ResultCode.SUCCESS);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			webResult.setResultCode(ResultCode.FAILURE);
			webResult.setFailure(e.getMessage());
		}
		return webResult;
	}
	
	@ResponseBody
	@RequestMapping(value = "/qqs", method = RequestMethod.GET)
	public WebResult readQQNodeByNickname(String nickname) {
		WebResult webResult = new WebResult();
		try {
			webResult.setData(qqRelationService.readQQNodeDataListByNickname(nickname));
			webResult.setResultCode(ResultCode.SUCCESS);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			webResult.setResultCode(ResultCode.FAILURE);
			webResult.setFailure(e.getMessage());
		}
		return webResult;
	}
	
	@ResponseBody
	@RequestMapping(value = "/quns", method = RequestMethod.POST)
	public String insertQunNode(String nodeJSON) {
		try {
			qqRelationService.insertQQQunNode(nodeJSON);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return e.getMessage();
		}
		return "success";
	}
	
	@ResponseBody
	@RequestMapping(value = "/quns/{qunNum}", method = RequestMethod.GET)
	public WebResult readQunNode(@PathVariable("qunNum") String qunNum) {
		WebResult webResult = new WebResult();
		try {
			webResult.setData(qqRelationService.readQunNodeDataList(qunNum));
			webResult.setResultCode(ResultCode.SUCCESS);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			webResult.setResultCode(ResultCode.FAILURE);
			webResult.setFailure(e.getMessage());
		}
		return webResult;
	}
	
	@ResponseBody
	@RequestMapping(value = "/relations", method = RequestMethod.POST)
	public String insertRelationNode(String nodeJSON) {
		try {
			qqRelationService.insertQQQunRelation(nodeJSON);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return e.getMessage();
		}
		return "success";
	}
	
}
