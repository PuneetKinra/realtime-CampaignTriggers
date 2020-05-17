package com.yesbank.camptriggers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataBaseDAO {
	
	private DataBaseDAO(String url, String username, String password) throws Exception{
		super();
		this.url = url;
		this.username = username;
		this.password = password;
		try {
                        Class.forName("oracle.jdbc.driver.OracleDriver");
                        this.connection = DriverManager.getConnection(this.url,this.username,this.password);
                } catch (ClassNotFoundException ex) {
                        System.out.println("Database Connection Creation Failed : " + ex.getMessage());
                }
	}

	private static DataBaseDAO instance;
	private Connection connection;
	private String url; 
	private String username ;
	private String password ;
	private static Object mutex;


	private DataBaseDAO() throws Exception {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			this.connection = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException ex) {
			throw new Exception(ex);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public static DataBaseDAO getInstance(String url, String username, String password) throws Exception {
		if(instance ==null)
		{
			synchronized (DataBaseDAO.class) {
				if (instance == null) {
					instance = new DataBaseDAO(url,username,password);
				}
			}
		}

		return instance;
	}

}
