package org.platform.modules.mapreduce.clean.xx.mysql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ReadAndFindFile {
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException{
		findTheRecord();
//		findLimitTable();
//		statisticsCount();
	}
	
	/**查询指定某行记录*/
	public static void findTheRecord() throws SQLException, ClassNotFoundException, IOException {
		MySqlClient client = 	new MySqlClient();
		Connection conn = (Connection) client.connection;
		Statement sql=conn.createStatement();
		//用文件流读取表名
		InputStreamReader in = new InputStreamReader(new FileInputStream("D:\\表名.txt"));  
		BufferedReader br=new BufferedReader(in);
        String line = "";
        List<String> listTable = new  ArrayList<String>();
        while((line=br.readLine())!=null){
        	String[] tableName = line.split(",");
        	for(String str:tableName){
        		listTable.add(str);
        	}
        }
        //将查询结果写入到指定文件里
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\查询结果.txt")));
        for(String tb:listTable){
//        	ResultSet rs=sql.executeQuery("select * from "+tb);
        	ResultSet rs=sql.executeQuery("select * from "+tb+" limit 1");
//        	ResultSet rs=sql.executeQuery("select substring_index(c2,' ',1),case when instr(c2,' ')=0 then '' else substring_index(c2,' ',-1) end,c3,c4,c5,c6,c7,c8,c9,c10,c40,sourceFile,updateTime,'liuyu' from "+tb);
//        	ResultSet rs=sql.executeQuery("SELECT substring_index(c1,' ',1), case when instr(c1,' ')=0 then '' else substring_index(c1,' ',-1)end,substring_index(c2,' ',1),substring_index(c2,' ',-2),substring_index(c3,' ',-1),substring_index(c4,' ',1),substring_index(c4,' ',-1),case when instr(c5,' ')=0 then '' else substring_index(c5,' ',-1)end,substring_index(c6,' ',1),substring_index(c6,' ',-1),c8 FROM "+tb);
        	StringBuilder sb = new StringBuilder();
    		while(rs.next()){
    			for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
    				sb.append(rs.getObject(i)+"	");
    			}
			bw.write(sb.toString()+"	"+tb);
			bw.newLine();
			sb = new StringBuilder();
    		}
        }
        System.out.println("完毕");
        br.close();
        bw.close();
        conn.close();
	}
	
	/**根据字段查找相同的表*/
	public static void findLimitTable() throws SQLException, ClassNotFoundException, IOException {
		MySqlClient client = 	new MySqlClient();
		Connection conn = (Connection) client.connection;
		Statement sql=conn.createStatement();
		//用文件流读取表名
		InputStreamReader in = new InputStreamReader(new FileInputStream("D:\\表名.txt"));  
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
        //指定字段限制集合
        List<String> limitList = new ArrayList<String>();
        limitList.add("占位置");
        String str="新增日期	大股东	股东	总代理	代理商	帐号	真实姓名	币别	余额	存款次数	提款次数	存款总数	提款总数	国家	电话号码	电子邮箱	QQ	状态";
        String[] fileName = str.split("\t");
        for(String st:fileName){
        	limitList.add(st);
        }
        
        //将查询结果写入到指定文件里
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\查询结果.txt")));
        for(String tb:listTable){
        	ResultSet rs=sql.executeQuery("select * from "+tb+" limit 1");
        	StringBuilder sb = new StringBuilder();
    		while(rs.next()){
    			 int times=0;
    			for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
    				sb.append(rs.getObject(i)+"	");
    				if(i<limitList.size() &&limitList.get(i).equals(rs.getObject(i))){
    					times++;
    				}
    			}
    			if(times>17){
//    				bw.write(sb.toString()+"	");
//    				bw.newLine();
    				bw.write(tb+",");
    			}
    			sb = new StringBuilder();
    		}
        }
        System.out.println("完毕");
        br.close();
        bw.close();
        conn.close();
	}
	
	
	/**指定表的所有记录总数
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws IOException */
	public static void statisticsCount() throws ClassNotFoundException, SQLException, IOException{
		MySqlClient client = 	new MySqlClient();
		Connection conn = (Connection) client.connection;
		Statement sql=conn.createStatement();
		//用文件流读取表名
		InputStreamReader in = new InputStreamReader(new FileInputStream("D:\\表名.txt"));  
		BufferedReader br=new BufferedReader(in);
        String line = "";
        List<String> listTable = new  ArrayList<String>();
        while((line=br.readLine())!=null){
        	String[] tableName = line.split(",");
        	for(String str:tableName){
        		listTable.add(str);
        	}
        }
        int count=0;
        for(String tb:listTable){
        	ResultSet rs=sql.executeQuery("select count(*) totalCount from "+tb);
        	if(rs.next()){
        		count+=rs.getInt("totalCount");
        		System.out.println(rs.getInt("totalCount")+"	"+tb);
        	}
        }
        System.out.println("总记录数为："+count);
        br.close();
        conn.close();
	}
}
