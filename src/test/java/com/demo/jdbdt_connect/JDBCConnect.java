package com.demo.jdbdt_connect;

import java.sql.*;  
public class JDBCConnect {
	
	public static void main(String args[]) {
		
		try{  
		Class.forName("com.amazon.redshift.jdbc42.Driver");  
		Connection con=DriverManager.getConnection(  
		"jdbc:postgresql://172.31.122.43:5439/datamart_qa","qa_user","Epsap@2018");  
		
		System.out.println(con);
		//Statement stmt=con.createStatement();  
		//ResultSet rs=stmt.executeQuery("select * from emp");  
		//while(rs.next())  
		//System.out.println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3));  
		con.close();  
		}catch(Exception e){ System.out.println(e);}  
	}  
	
}
