package com.yesbank.camptriggers;


import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.namespace.QName;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPBodyElement;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static com.yesbank.camptriggers.UtilsFunction.*;

public class App {


	public static void main(String[] args) {

		try {
			if (args.length == 0) {
				System.out.println("PropertiesStream.json File Missing");
				System.exit(-1);
			}
			String propertyFile = args[0];
			JsonObject jsonFileReader = JsonFileReader(propertyFile);

			//Fetching the list of topics
			List<String> topics = Arrays.asList(jsonFileReader.get("AppConfiguration").
					getAsJsonObject().get("topics").getAsString().split(","));
			//Fetching hostnames
			String hostnames=jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("hostnames").getAsString();
			//Fetching groupid
			String groupid=jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("groupid").getAsString();
			//Fetching ThreadNumber
			int threadNumber=jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("counsumerThreads").getAsInt();

			String  audittopic=jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("audittopic").getAsString();


			String serviceName =jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("servicename").getAsString();

			//Message center information
			String userName=jsonFileReader.get("messagecenter").
				getAsJsonObject().get("username").getAsString();
			String passWord=jsonFileReader.get("messagecenter").
				getAsJsonObject().get("password").getAsString();
			String endpoint=jsonFileReader.get("messagecenter").
				getAsJsonObject().get("endpoint").getAsString();
			String soapAction=jsonFileReader.get("messagecenter").
				getAsJsonObject().get("soapaction").getAsString();

			//Database information
			final String dburl=jsonFileReader.get("datasource").
				getAsJsonObject().get("url").getAsString();
			final String dbuser=jsonFileReader.get("datasource").
				getAsJsonObject().get("username").getAsString();
			final String dbpwd=jsonFileReader.get("datasource").
				getAsJsonObject().get("password").getAsString();
			String dbquery=jsonFileReader.get("datasource").
				getAsJsonObject().get("query").getAsString();

			//log files
			String logpath=jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("logging").getAsJsonObject().get("logpath").getAsString();
			String loglevel=jsonFileReader.get("AppConfiguration").
				getAsJsonObject().get("logging").getAsJsonObject().get("loglevel").getAsString();


			final ExecutorService executor = Executors.newFixedThreadPool(threadNumber);
			final List<EventsConsumer>threads=new ArrayList<EventsConsumer>();
			for(int threadN=1;threadN<=threadNumber;threadN++) {
				EventsConsumer eventsConsumer= new EventsConsumer(hostnames,groupid,"false","6000",
						200,userName+"/"+passWord,endpoint, topics,
						soapAction,dburl,dbuser,dbpwd,dbquery,logpath,loglevel,"2000",audittopic,serviceName);
				threads.add(eventsConsumer);
				executor.execute(eventsConsumer);
			}



			System.out.println("ShutDown Hook Called..");
			//For Shuting down  the App

			Runtime.getRuntime().addShutdownHook(new Thread() 
					{ 

					public void run() 
					{ 
					System.out.println("ShutDown Hook Called..");
					DataBaseDAO instance;
					try {
					System.out.println("Closing the Database connection");
					instance = DataBaseDAO.getInstance(dburl,dbuser,dbpwd);
					Connection connection = instance.getConnection();
					connection.close();
					} catch (Exception e1) {
					// TODO Auto-generated catch block
					System.out.println("Exception While Closing the Database connection");

					}

					for(EventsConsumer eventConsumerThread:threads) {
					eventConsumerThread.shutdown();
					}
					executor.shutdown();
					try {
						executor.awaitTermination(10,TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						System.out.println("Exception While awaiting for the Termination of Threads"+e);
					}

					} 
					});  
		}catch(Exception e) {
			System.out.println("Exeception in main during submission of the job"+e);
		}

	}
}


