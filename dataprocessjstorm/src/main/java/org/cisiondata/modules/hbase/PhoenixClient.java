package org.cisiondata.modules.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PhoenixClient {
	
	private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
	
	private static String url = "jdbc:phoenix:192.168.0.124:2181";
	
	static {
		try {
			Class.forName(driver);
		} catch	(ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Connection connection = null;
		try {
			System.out.println("start connect phoenix");
			connection = DriverManager.getConnection(url);
			System.out.println("end connect phoenix");
		} catch	(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}
