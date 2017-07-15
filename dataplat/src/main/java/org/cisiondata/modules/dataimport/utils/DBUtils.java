package org.cisiondata.modules.dataimport.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtils {
	/**
     * 获取Connection
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
	  public static Connection getConnection(String dbname){
		  String url="jdbc:mysql://192.168.0.125:3306/"+dbname+"?useUnicode=true&characterEncoding=UTF-8";
		  String usr="root";
		  String pwd="root";
		  Connection conn=null;
		   try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, usr, pwd);
		} catch (Exception e) {
			 System.out.println("获取数据库连接失败！");
		}
		   return conn;
	  }
	    /**
	     * 关闭ResultSet
	     * @param rs
	     */
	    public static void closeResultSet(ResultSet rs) {
	        if (rs != null) {
	            try {
	                rs.close();
	            } catch (SQLException e) {
	                System.out.println(e.getMessage());
	            }
	        }
	    }
	     
	    /**
	     * 关闭Statement
	     * @param stmt
	     */
	    public static void closeStatement(Statement stmt) {
	        if (stmt != null) {
	            try {
	                stmt.close();
	            }       
	            catch (Exception e) {
	                System.out.println(e.getMessage());
	            }
	        }
	    }
	     
	    /**
	     * 关闭ResultSet、Statement
	     * @param rs
	     * @param stmt
	     */
	    public static void closeStatement(ResultSet rs, Statement stmt) {
	        closeResultSet(rs);
	        closeStatement(stmt);
	    }
	     
	    /**
	     * 关闭PreparedStatement
	     * @param pstmt
	     * @throws SQLException
	     */
	    public static void fastcloseStmt(PreparedStatement pstmt) throws SQLException
	    {
	        pstmt.close();
	    }
	     
	    /**
	     * 关闭ResultSet、PreparedStatement
	     * @param rs
	     * @param pstmt
	     * @throws SQLException
	     */
	    public static void fastcloseStmt(ResultSet rs, PreparedStatement pstmt) throws SQLException
	    {
	        rs.close();
	        pstmt.close();
	    }
	     
	    /**
	     * 关闭ResultSet、、Connection
	     * @param rs
	     * @param con
	     */
	    public static void closeConnection(ResultSet rs, Connection con)
	    {
	        try {
				rs.close();
				 con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	       
	    }
	    
	    
	    
	    /**
	     * 关闭ResultSet、PreparedStatement、Connection
	     * @param rs
	     * @param pstmt
	     * @param con
	     */
	    public static void closeConnection(ResultSet rs, PreparedStatement pstmt, Connection con) {
	        try {
	        	closeConnection(con);
		    	closeResultSet(rs);
				fastcloseStmt(pstmt);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	        
	    }
	     
	    /**
	     * 关闭Statement、Connection
	     * @param stmt
	     * @param con
	     */
	    public static void closeConnection(Statement stmt, Connection con) {
	        closeStatement(stmt);
	        closeConnection(con);
	    }  
	    /**
	     * 关闭Connection
	     * @param con
	     */
	    public static void closeConnection(Connection con) {
	        if (con != null) {
	            try {
	               con.close();
	            }
	            catch (Exception e) {
	                System.out.println(e.getMessage());
	            }
	        }
	    }
	  
	  
}
