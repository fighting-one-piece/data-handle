package org.cisiondata.modules.data.dao;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;
@Repository("dbDAO")
public interface DbDAO {
	//获取全部数据库
	public List<Map<String,Object>> readDbName() throws Exception;
	//获取数据库全部表名
	public List<String>  readTablename(String dbName)throws Exception;
	//获取表的列
	public List<String> readTableColumns(String tableName)throws Exception;
	
	public List<Map<String,Object>> readDatas(@Param("tableName")String tableName,@Param("row")String row) throws Exception;
}
