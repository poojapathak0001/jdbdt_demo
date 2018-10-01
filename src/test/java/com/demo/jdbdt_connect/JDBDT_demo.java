package com.demo.jdbdt_connect;

import org.jdbdt.DB;
//JDBDT import
import static org.jdbdt.JDBDT.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import org.jdbdt.TableBuilder;

public class JDBDT_demo {
	
	static //Connection
	Connection conn = null;
	
	// JDBDT handle for the database 
	  static DB theDB; 
		  
	// User table.
	  static TableBuilder theTable;
	
	private static final String JDBC_DRIVER_CLASS = "org.postgresql.Driver";
	static String UID = "qa_user";
	static String pass = "Epsap@2018";

	private static final String DATABASE_URL = "jdbc:postgresql://172.31.122.43:5439/datamart_qa";
	
	public static void setConnection () {
		String sql = "select * from datamart.topic_dim where netbase_topic_id = ?";
		PreparedStatement preparedStatement;
		try {
			conn = theDB.getConnection();
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setString(1, "877227");
			ResultSet rs = null;
			// execute SQL statement
			rs = preparedStatement.executeQuery();
			System.out.println(" Netbase Topic Name\t:\tDisplay Topic Name");
			System.out.println("---------------------------------------------------\n");
			while(rs.next()) {
				System.out.println(rs.getString("netbase_topic_name") + "\t:\t" + rs.getString("display_topic_name"));
			}
			

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("_____________________________________________________________\n");
		getTable();
		
	}
	
	//get table
	public static void getTable() {
		
		String sqlgettable = "select tablename from pg_catalog.pg_tables where schemaname != ? and schemaname != ? and schemaname = ?";
		try {
			PreparedStatement tables = conn.prepareStatement(sqlgettable);
			tables.setString(1, "information_schema");
			tables.setString(2, "pg_catalog");
			tables.setString(3, "datamart");
			ResultSet rs = null;
			// execute SQL statement
			rs = tables.executeQuery();
			System.out.println(" Tables");
			System.out.println("-----------\n");
			while(rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTableExists(theDB, "geo_dim");
		
	}

	public static void globalSetup() throws Throwable {
		 // Load JDBC driver class
	    Class.forName(JDBC_DRIVER_CLASS);
	    
	    // Create database handle
	    theDB = database(DATABASE_URL,UID,pass);
	  }
	
	public static void main(String args[]) throws Throwable {
		
		globalSetup();
		setConnection();
	}
}
