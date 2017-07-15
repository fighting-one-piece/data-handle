package org.cisiondata.modules.dataimport.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cisiondata.modules.dataimport.service.DatabaseQueryService;
import org.cisiondata.modules.dataimport.utils.DBUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service("databaseQueryService")
public class DatabaseQueryServiceImpl implements DatabaseQueryService {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	/**数据库名查询*/
	@Override
	public List<String> findDbName() {
		List<String> list = new ArrayList<String>();
		Connection con = null;
		ResultSet rs = null;
		try {
			con = jdbcTemplate.getDataSource().getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			rs = dmd.getCatalogs();
			while (rs.next()) {
				list.add(rs.getString("TABLE_CAT"));
			}
		} catch (SQLException e) {

			e.printStackTrace();
		} finally {
			DBUtils.closeConnection(rs, con);
		}
		return list;
	}
	/**数据库表名查询*/
	@Override
	public List<String> findTbName(String dbname) {
		List<String> list=new ArrayList<String>();
		Connection con=null;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		try {
			con = jdbcTemplate.getDataSource().getConnection();
			pstmt=con.prepareStatement
		    		  ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?");
			pstmt.setString(1,dbname);
			 rs=pstmt.executeQuery();
			  while(rs.next()){
			   list.add(rs.getString("TABLE_NAME"));
			  }
		} catch (SQLException e) {			
			e.printStackTrace();
		}finally{
			
			DBUtils.closeConnection(rs, pstmt, con);
		}
		return list;
	}
	@Override
	public List<Map<Object, Object>> SelectData(String tbname, String dbName) {
	List<Map<Object, Object>> list = new ArrayList<Map<Object, Object>>();
	List<String> columnNames = new ArrayList<String>();
	Connection con = null;
	PreparedStatement pstmt = null;
	Map<Object, Object> map = null;
	ResultSet rs = null;
	try {
		con = DBUtils.getConnection(dbName);
		pstmt = con.prepareStatement("select * from " + tbname + " limit 0,100 ");
		rs = pstmt.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		int numberOfColumns = rsmd.getColumnCount();
		for (int i = 1; i <= numberOfColumns; i++) {
			String columnName = rsmd.getColumnName(i);
			columnNames.add(columnName);
		}
		while (rs.next()) {
			map = new HashMap<Object, Object>();
			for (int i = 1; i <= numberOfColumns; i++) {
				String columnName = rsmd.getColumnName(i);
				Object object = rs.getObject(columnName);
				map.put(columnName, object);
			}
			list.add(map);
		}
	} catch (Exception e) {
		e.printStackTrace();
	} finally {
		DBUtils.closeConnection(rs, pstmt, con);
	}
	Map<Object, Object> columnNameMap = new HashMap<Object, Object>();
	columnNameMap.put("columnNameMap", columnNames);
	list.add(columnNameMap);
	return list;
	}
	

}
