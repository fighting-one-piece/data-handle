package org.cisiondata.modules.elastic.controller;

import javax.annotation.Resource;

import org.cisiondata.modules.elastic.service.IElasticService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/elastic5")
public class ElasticController {

	private Logger LOG = LoggerFactory.getLogger(ElasticController.class);
	
	@Resource(name = "elasticService")
	private IElasticService elasticService = null;
	
	@RequestMapping(value = "/index/{index}/type/{type}/bulk", method = RequestMethod.POST)
	public String bulkInsert(@PathVariable("index") String index, @PathVariable("type") String type, String source) {
		try {
			elasticService.bulkInsert(index, type, source);
		} catch (Exception e) {
			LOG.error("index:{} type:{} source:{}", index, type, source);
			LOG.error(e.getMessage(), e);
			return e.getMessage();
		}
		return "success";
	}
	
}
