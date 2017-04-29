package org.cisiondata.modules.scheduler.service.impl;

import java.util.List;

import javax.annotation.Resource;

import org.cisiondata.modules.qqrelation.service.IQQGraphService;
import org.cisiondata.modules.scheduler.service.IConsumeService;
import org.springframework.stereotype.Service;

@Service("qqNodeConsumeService")
public class QQNodeConsumeServiceImpl implements IConsumeService {
					  
	@Resource(name = "qqGraphService")
	private IQQGraphService qqGraphService = null;
	
	@Override
	public void handle(String message) throws RuntimeException {
		System.out.println("qq node consumer: " + message);
		qqGraphService.insertQQNode(message);
	}
	
	@Override
	public void handle(List<String> messages) throws RuntimeException {
		for (int i = 0, len = messages.size(); i < len; i++) {
			System.out.println("qq node consumer: " + messages.get(i));
		}
		System.out.println("handle finish!!!!!!");
		qqGraphService.insertQQNodes(messages);
	}
	
	
	
}
