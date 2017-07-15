package org.cisiondata.modules.data.service;

import java.util.Map;



public interface DbService {
	
	public  Map<String,Object> readDbNames()  throws Exception;  
	
	public  Map<String,Object> readTableNames(String dbName) throws Exception;
	
	public  Map<String,Object> readColumns(String table)throws Exception;
	
	public Map<String,Object> readDatas(String table,String type)throws Exception;
}
