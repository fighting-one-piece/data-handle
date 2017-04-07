package org.platform.modules.mapreduce.clean.xx.mysql;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DeleteRecord {
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		MySqlClient client = 	new MySqlClient();
		Connection conn = (Connection) client.connection;
		Statement sql=conn.createStatement();
		//用文件流读取表名
				InputStreamReader in = new InputStreamReader(new FileInputStream("D:\\表名2.txt"));  
				BufferedReader br=new BufferedReader(in);
		        String line = "";
		        List<String> listTable = new  ArrayList<String>();
		        while((line=br.readLine())!=null){
		        	String[] tableName = line.split(",");
		        	for(String str:tableName){
		        		listTable.add(str);
		        		System.out.println(str);
		        	}
		        }
		        for(String tb:listTable){
//		        	sql.execute("delete from "+tb+" limit "+1+";");
		        	sql.execute("delete from "+tb+" where c1='新增日期'");
//		        	sql.execute("delete from "+tb+" where c1='编号' and c2='帐号密码'");
//		        	sql.execute("DROP TABLE "+tb);
		    		}
		        System.out.println("删除完毕");
		        br.close();
		        conn.close();
		        }
	}
