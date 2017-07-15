package org.cisiondata.modules.dataimport.service;

import java.util.List;
import java.util.Map;

public interface DatabaseQueryService {
  /**数据库名查询*/
	public List<String>findDbName();
	
  /**数据库表名查询*/
  public List<String>findTbName(String dbname);	 
  
  /**查询数据表前100条*/
  public List<Map<Object, Object>> SelectData(String tbname, String dbName);
}
