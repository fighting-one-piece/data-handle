package org.cisiondata.modules.elastic.service;

public interface IElasticService {

	public void bulkInsert(String index, String type, String source) throws RuntimeException;
	
}
