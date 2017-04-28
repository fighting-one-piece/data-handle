package org.cisiondata.modules.kafka.service;

public interface IConsumeService {
	
	public void handle(String message) throws RuntimeException;
	
}
