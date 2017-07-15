package org.cisiondata.modules.dataimport.controller;

import javax.annotation.Resource;

import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.abstr.web.WebResult;
import org.cisiondata.modules.dataimport.service.IESMetadataService;
import org.cisiondata.utils.exception.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class ESMetadataController {

	private Logger LOG = LoggerFactory.getLogger(ESMetadataController.class);
	
	@Resource(name = "esMetadataService")
	private IESMetadataService esMetadataService = null;
	
	@RequestMapping(value = "/indices",method=RequestMethod.GET)
	public WebResult readIndices(){
		WebResult result = new WebResult();
		try {
			result.setData(esMetadataService.readIndices());
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch (BusinessException bu) {
			LOG.error(bu.getDefaultMessage(),bu);
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e){
			LOG.error(e.getMessage(),e);
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}
	
	@RequestMapping(value = "/types",method=RequestMethod.GET)
	public WebResult readIndexTypes(String index){
		WebResult result = new WebResult();
		try {
			result.setData(esMetadataService.readIndexTypes(index));
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch (BusinessException bu) {
			LOG.error(bu.getDefaultMessage(),bu);
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e){
			LOG.error(e.getMessage(),e);
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}
	
	@RequestMapping(value = "/attributes",method=RequestMethod.GET)
	public WebResult readIndexTypeAttributes(String index,String type){
		WebResult result = new WebResult();
		try {
			result.setData(esMetadataService.readIndexTypeAttributes(index, type));
			result.setCode(ResultCode.SUCCESS.getCode());
		} catch (BusinessException bu) {
			LOG.error(bu.getDefaultMessage(),bu);
			result.setCode(bu.getCode());
			result.setFailure(bu.getDefaultMessage());
		} catch (Exception e){
			LOG.error(e.getMessage(),e);
			result.setCode(ResultCode.SYSTEM_IS_BUSY.getCode());
			result.setFailure(ResultCode.SYSTEM_IS_BUSY.getDesc());
		}
		return result;
	}
	
}
