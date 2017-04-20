package org.cisiondata.modules.qqrelation.controller;

import java.util.List;

import javax.annotation.Resource;

import org.cisiondata.modules.qqrelation.service.IQQRelationService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QQRelationController {

	@Resource(name = "qqRelationService")
	private IQQRelationService qqRelationService = null;
	
	@ResponseBody
	@RequestMapping(value = "/qqs", method = RequestMethod.POST)
	public String insertQQBatch(List<String> nodes) {
		qqRelationService.insertQQNodes(nodes);
		return "success";
	}
	
}
