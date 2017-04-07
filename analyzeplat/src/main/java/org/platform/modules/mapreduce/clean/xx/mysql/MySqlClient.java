package org.platform.modules.mapreduce.clean.xx.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlClient {
	public static final String url = "jdbc:mysql://192.168.0.115:3306/ly20170213?useUnicode=true&characterEncoding=utf8";
	public static final String name = "com.mysql.jdbc.Driver";
	public static final String user = "root";
	public static final String password = "123";
	public Connection connection=null;
	
	public MySqlClient() throws ClassNotFoundException, SQLException{
		Class.forName("com.mysql.jdbc.Driver");
		 this.connection = DriverManager.getConnection(url, user, password);// 获取连接
	}
}
