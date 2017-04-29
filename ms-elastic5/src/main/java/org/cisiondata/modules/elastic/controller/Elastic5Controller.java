package org.cisiondata.modules.elastic.controller;

import javax.annotation.Resource;

import org.cisiondata.modules.elastic.service.IElastic5Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/elastic5")
public class Elastic5Controller {

	public Logger LOG = LoggerFactory.getLogger(Elastic5Controller.class);
	
	@Resource(name = "elastic5Service")
	private IElastic5Service elastic5Service = null;
	
	
	
}
