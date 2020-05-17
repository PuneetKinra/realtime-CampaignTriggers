package com.yesbank.camptriggers;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import javax.xml.namespace.QName;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPBodyElement;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;



public class EventsConsumer implements Runnable{

	private String loggingpath;
	private String loglevel;
	private Properties properties;
	private long polltimeout;
	private String password;
	private String SOAPAction;
	private String endpointUrl;
	private List<String> topics;
	private KafkaConsumer<String, String> consumer;
	private Map<TopicPartition, OffsetAndMetadata> offsets;
	private String dbpassword;
	private String dbuser;
	private String dburl;
	private String dbquery;	
	private Logger logger;
	private String threadName;
	private AuditProducer producer;
	private String auditTopic;

	public EventsConsumer(String bootstrapServers, String groupId,String enableAutoCommit, String sessionTimeoutMs,long polltimeout,String password,String endpointUrl,List<String>topics,String SOAPAction,String dburl,String dbuser,String dbpassword,String dbquery,String loggingpath,String loglevel,String heartbeat,String auditTopic,String serviceName) throws Exception {
		super();
		properties=new Properties();
		offsets=new HashMap<TopicPartition,OffsetAndMetadata>();
		properties.setProperty("bootstrap.servers",bootstrapServers);
		properties.setProperty("group.id", groupId);
		properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("enable.auto.commit", enableAutoCommit);
		properties.setProperty("session.timeout.ms", sessionTimeoutMs);
		properties.setProperty("auto.offset.reset","latest");
		properties.setProperty("heartbeat.interval.ms",heartbeat);	
		properties.setProperty("security.protocol","SASL_PLAINTEXT");
		properties.setProperty("sasl.kerberos.service.name",serviceName);
		this.polltimeout=polltimeout;
		this.password=password;
		this.endpointUrl=endpointUrl;
		this.topics=topics;
		this.SOAPAction=SOAPAction;
		this.dburl=dburl;
		this.dbuser=dbuser;
		this.dbpassword=dbpassword;
		this.dbquery=dbquery;
		this.loggingpath =loggingpath;
		this.loglevel =loglevel;	
		this.auditTopic=auditTopic;
		try {
			this.producer=new AuditProducer(bootstrapServers,auditTopic,serviceName);
		}catch(Exception producerExcep) {
			throw new Exception(producerExcep);
		}


	}

	public Logger getLogger(String fileName,String appenderName,String logLevel) {
		Logger customLogger;
		String conversionPattern = "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n";
		PatternLayout layout = new PatternLayout();
		layout.setConversionPattern(conversionPattern);
		// creates file appender
		RollingFileAppender rollingFileAppender = new RollingFileAppender();
		rollingFileAppender.setFile(fileName);
		rollingFileAppender.setLayout(layout);
		rollingFileAppender.activateOptions();
		rollingFileAppender.setName(appenderName);
		rollingFileAppender.setMaxFileSize("1MB");
		rollingFileAppender.setMaxBackupIndex(100);
		rollingFileAppender.setAppend(true);

		// configures the root logger

		customLogger = Logger.getLogger(appenderName);
		if(logLevel.equalsIgnoreCase("DEBUG"))
			customLogger.setLevel(Level.DEBUG);
		else if(logLevel.equalsIgnoreCase("ERROR"))
			customLogger.setLevel(Level.ERROR);
		else if(logLevel.equalsIgnoreCase("INFO"))
			customLogger.setLevel(Level.INFO);
		customLogger.addAppender(rollingFileAppender);	
		return customLogger;
	}


	//Function To Extract the information from the database for customer details and channel
	public  ArrayList<Event> getDBEvents(Long customerId,String eventId,Logger applicationlogger) throws Exception{
		//Will build the Query and extract the data.
		Connection conn;
		PreparedStatement  stmt=null;
		ResultSet results=null;
		try {
			ArrayList<Event> eventsList = new ArrayList<Event>();
			//@Execption check
			DataBaseDAO instance = DataBaseDAO.getInstance(dburl,dbuser,dbpassword);
			//@Execption check
			conn= instance.getConnection();
			//@Exception check
			stmt =conn.prepareStatement(dbquery);
			//@Exception check
			stmt.setString(1,String.valueOf(customerId));
			//@Exception check
			stmt.setString(2,String.valueOf(eventId));
			//@Exception check
			results = stmt.executeQuery();
			ResultSetMetaData rsmd = results.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			//@Exception check over the loop
			while (results.next()) {
				//Email.SMS and PUSH

				if(results.getString("WISHCHANNEL").equals("0") && (results.getString("EVTSTATUS")!=null && ! results.getString("EVTSTATUS").isEmpty()) 
						&& 
						(results.getString("SREF_CUST_EMAIL")!=null && ! results.getString("SREF_CUST_EMAIL").isEmpty())) {
					Event event=new Event();
					event.setEventname(results.getString("EVTSTATUS"));
					event.setChannelId(results.getString("WISHCHANNEL"));
					event.setDestinationAttribute(results.getString("SREF_CUST_EMAIL"));
					event.setChannelName("email");
					event.setValid(true);
					Map<String,String>customAttr=new HashMap<String,String>();
					for (int index = 1; index <= columnsNumber; index++) {
						if (index > 1) {
							customAttr.put(rsmd.getColumnName(index),results.getString(index));
						}

					}
					event.setCustomAttr(customAttr);
					eventsList.add(event);
				}else if(results.getString("WISHCHANNEL").equals("2") && (results.getString("EVTSTATUS")!=null && ! results.getString("EVTSTATUS").isEmpty()) 
						&& 
						(results.getString("SREF_CUST_TELEX")!=null && ! results.getString("SREF_CUST_TELEX").isEmpty())) {
					Event event=new Event();
					event.setEventname(results.getString("EVTSTATUS"));
					event.setChannelId(results.getString("WISHCHANNEL"));
					event.setDestinationAttribute(results.getString("SREF_CUST_TELEX"));
					event.setChannelName("mobilePhone");
					event.setValid(true);
					Map<String,String>customAttr=new HashMap<String,String>();
					for (int index = 1; index <= columnsNumber; index++) {
						if (index > 1) {
							customAttr.put(rsmd.getColumnName(index),results.getString(index));
						}

					}
					event.setCustomAttr(customAttr);
					eventsList.add(event);
				}else if(results.getString("WISHCHANNEL").equals("41") && (results.getString("EVTSTATUS")!=null && ! results.getString("EVTSTATUS").isEmpty()) 
						&&
						(results.getString("SREGISTRATIONTOKEN")!=null && ! results.getString("SREGISTRATIONTOKEN").isEmpty())) {
					Event event=new Event();
					event.setEventname("DELIVERY");
					event.setChannelId(results.getString("WISHCHANNEL"));
					event.setDestinationAttribute(results.getString("SREGISTRATIONTOKEN"));
					event.setChannelName("registrationToken");
					event.setUniqueUserid("com.neolane.NeoMiles");
					event.setValid(true);
					Map<String,String>customAttr=new HashMap<String,String>();
					for (int index = 1; index <= columnsNumber; index++) {
						if (index > 1) {
							customAttr.put(rsmd.getColumnName(index),results.getString(index));
						}

					}
					event.setCustomAttr(customAttr);

					eventsList.add(event);
				}else if(results.getString("WISHCHANNEL").equals("42") && (results.getString("EVTSTATUS")!=null && ! results.getString("EVTSTATUS").isEmpty()) 
						&&
						(results.getString("SREGISTRATIONTOKEN")!=null && ! results.getString("SREGISTRATIONTOKEN").isEmpty())){

					Event event=new Event();
					event.setEventname("DELIVERY");
					event.setChannelId(results.getString("WISHCHANNEL"));
					//Need to add
					event.setDestinationAttribute(results.getString("SREGISTRATIONTOKEN"));
					event.setChannelName("registrationToken");
					//Need to change
					event.setUniqueUserid("com.neolane.NeoMiles");
					event.setValid(true);
					Map<String,String>customAttr=new HashMap<String,String>();
					for (int index = 1; index <= columnsNumber; index++) {
						if (index > 1) {
							customAttr.put(rsmd.getColumnName(index),results.getString(index));
						}

					}
					event.setCustomAttr(customAttr);
					eventsList.add(event);
				}

			}
			//IOS and Mobile Push
			return eventsList;
		}catch(Exception exp) {
			throw new Exception(exp);
		}finally {
			stmt.close();
			results.close();

		}

	}


	//Push Event Request generation
	public  Request pushEvent(String password,JsonObject ctx,Event event,Logger applicationlogger) throws Exception {
		try {
			//@Exception check
			MessageFactory factory = MessageFactory.newInstance();
			//@Exception check
			SOAPMessage soapMsg = factory.createMessage();
			SOAPPart part = soapMsg.getSOAPPart();
			//SETTING UP THE NAMESPACES
			//@Exception check
			SOAPEnvelope envelope = part.getEnvelope();
			envelope.removeNamespaceDeclaration(envelope.getPrefix());
			//@Exception check
			envelope.addNamespaceDeclaration("soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
			//@Exception check
			envelope.addNamespaceDeclaration("urn", "urn:nms:rtEvent");
			//@Exception check
			envelope.setPrefix("soapenv");
			//NO HEADER APPENDING
			//@Exception check
			SOAPHeader header = envelope.getHeader();
			//@Exception check
			header.setPrefix("soapenv");
			//BODY MANIPULATION STARTS FROM HERE
			//@Exception check
			SOAPBody body = envelope.getBody();
			//@Exception check
			body.setPrefix("soapenv");
			//CREATING PUSHEVENT
			QName pushEvent = new QName("urn:nms:rtEvent", "PushEvent", "urn");
			//@Exception check
			SOAPBodyElement pushEventDom = body.addBodyElement(pushEvent);
			//ADDING SESSIONTOKEN
			QName sessionToken = new QName("urn:nms:rtEvent", "sessiontoken", "urn");
			//@Exception check
			SOAPElement sessionT = pushEventDom.addChildElement(sessionToken);
			//@Exception check
			sessionT.addTextNode(password);
			//ADDING DOMEVENT
			QName domEvent = new QName("urn:nms:rtEvent", "domEvent", "urn");
			//@Exception check
			SOAPElement domEventDom = pushEventDom.addChildElement(domEvent);
			//ADDING RTEVENT
			QName rtEvent = new QName("rtEvent");
			//@Exception check
			SOAPElement rtEventDom = domEventDom.addChildElement(rtEvent);
			//@Exception check
			rtEventDom.addAttribute(new QName("type"),event.getEventname());
			//@Exception check
			rtEventDom.addAttribute(new QName("wishedChannel"),event.getChannelId());
			//@Exception check
			rtEventDom.addAttribute(new QName(event.getChannelName()),event.getDestinationAttribute());	
			if(event.getChannelId().equals("41") || event.getChannelId().equals("42"))
			{
				SOAPElement mobileApp = rtEventDom.addChildElement(new QName("mobileApp"));
				mobileApp.addAttribute(new QName("uuid"),event.getUniqueUserid());
			}
			// ctx event
			QName ctxEvent = new QName("ctx");
			//@Exception check
			SOAPElement ctxEventDom = rtEventDom.addChildElement(ctxEvent);
			//Personalized fields
			Set<Entry<String, JsonElement>> entrySet = ctx.entrySet();
			for(Map.Entry<String,JsonElement> entry : entrySet){
				ctxEventDom.addChildElement(new QName(entry.getKey())).addTextNode(entry.getValue().getAsString());
			}
			//customattr
			Set<Entry<String, String>> customattr = event.getCustomAttr().entrySet();

			for(Map.Entry<String,String> entry : customattr){
				if(entry.getValue()!=null && !entry.getValue().isEmpty())
					ctxEventDom.addChildElement(new QName(entry.getKey())).addTextNode(entry.getValue());
			}

			ByteArrayOutputStream outputStr = new ByteArrayOutputStream();
			//@Exception check
			soapMsg.writeTo(outputStr);
			String request = outputStr.toString("UTF-8");
			return new Request(request);
		}catch(Exception exp) {
			throw new Exception(exp);
		}
	}



	//Post Method
	public Response PostMethod(String endpointUrl,String SOAPAction ,String pushEventReq,Logger applicationlogger) throws Exception {
		try {
			PostMethod postMtd = new PostMethod(endpointUrl);
			postMtd.setRequestHeader("Content-type", "application/xml");
			postMtd.setRequestHeader("SOAPAction",SOAPAction);
			postMtd.setRequestBody(pushEventReq);
			HttpClient httpclient = new HttpClient();
			//@Exception check
			Long respCode = (long) httpclient.executeMethod(postMtd);
			//Need to close the client
			return new Response(respCode,postMtd.getResponseBodyAsString());
		}catch(Exception exp) {
			throw new Exception(exp);
		}
	}


	//Consumer method
	public void consume(final Logger applicationlogger) {
		try {
			consumer= new KafkaConsumer<String,String>(properties);
			consumer.subscribe(topics,new Rebalancer(consumer,offsets,applicationlogger));
			//need to add the rebalancer
			while(true) {	
				//poll the records
				ConsumerRecords<String, String> listrecords = consumer.poll(this.polltimeout);
				//iterate 
				for(ConsumerRecord <String, String>  record:listrecords) {
					//convert the json record string in jsonobject
					JsonObject inputPayload=null;
					try{
						applicationlogger.debug("Record Received:"+record);
						JsonParser jsonParser = new JsonParser();
						JsonElement value = jsonParser.parse(record.value());
						inputPayload = value.getAsJsonObject();
						ArrayList<Event> dbEvents=null;
						long customerId;
						String adobeEventId;
						JsonObject personalizationFields;

						//Checking Mandatory field missing.
						try {
							customerId = inputPayload.get("customer_id").getAsLong();
							adobeEventId=inputPayload.get("adobeevent_id").getAsString();	
							personalizationFields=inputPayload.get("personalization_fields").getAsJsonObject();
						}catch(Exception mandatoryField) {
							throw new Exception("Mandatory field customerId/AdobeEventId/personalization_fields is Missing:"+record);
						}
						//fetching db events using using customer id and adobeevent_id
						dbEvents = getDBEvents(customerId,adobeEventId,applicationlogger);
						for(Event event:dbEvents) {
							try {
								if(event.isValid()==false) {
									throw new Exception("Event is Not Valid Since Mandatory fields are missing");
								}	
								//@Exception check--Done Till here
								Request pushEvnt= pushEvent(this.password,personalizationFields,event,applicationlogger);
								//@Exception check
								Response response = PostMethod(this.endpointUrl,this.SOAPAction,pushEvnt.getValue(),applicationlogger);
								ProducerRecord<String, String> auditmessage = this.producer.auditmessage(inputPayload,0,response.getResponseMsg(),response.getResponseCode(),this.auditTopic);
								this.producer.produce(auditmessage);
								applicationlogger.debug("Auditmessage:"+auditmessage);
								applicationlogger.debug("Response received:"+response);
							} catch (SOAPException e) {
								ProducerRecord<String, String> auditmessage = this.producer.auditmessage(inputPayload,1,e.getMessage(),-99L,this.auditTopic);
								this.producer.produce(auditmessage);
								// TODO Auto-generated catch block
								applicationlogger.error("Exception caused while building post request and posting soap message:",e);
							} catch (IOException e) {
								ProducerRecord<String, String> auditmessage = this.producer.auditmessage(inputPayload,1,e.getMessage(),-99L,this.auditTopic);
								this.producer.produce(auditmessage);
								applicationlogger.error("Exception caused while building post request and posting soap message:",e);
							}catch (Exception  e) {
								ProducerRecord<String, String> auditmessage = this.producer.auditmessage(inputPayload,1,e.getMessage(),-99L,this.auditTopic);
								this.producer.produce(auditmessage);
								applicationlogger.error("Exception caused while building post request and posting soap message:",e);
							}

						}
						//Commiting the value
						TopicPartition key=new TopicPartition(record.topic(),record.partition());
						OffsetAndMetadata commitvalue=new OffsetAndMetadata(record.offset()+1);
						offsets.put(key,commitvalue);
						//Commiting Asyncronously & implementing the Callback
						consumer.commitAsync(offsets,new OffsetCommitCallback() {
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
								// TODO Auto-generated method stub
								if(exception !=null)
								{
									applicationlogger.error("Exception caused while commiting in Async Commit:",exception);
								}
							}
						});
					}catch(Exception e) {
						if(inputPayload!=null) {
							ProducerRecord<String, String> auditmessage = this.producer.auditmessage(inputPayload,1,e.getMessage(),-99L,this.auditTopic);
							applicationlogger.debug("Auditmessage:"+auditmessage);
							this.producer.produce(auditmessage);
							applicationlogger.error("Exception caused by:",e);
						}else {
							ProducerRecord<String, String> auditmessage = this.producer.auditmessage(new JsonObject(),1,e.getMessage(),-99L,this.auditTopic);
							applicationlogger.debug("Auditmessage:"+auditmessage);
							this.producer.produce(auditmessage);
							applicationlogger.error("Exception caused by:",e);
						}
					}
				}
			}
		}catch (WakeupException e) {
			// ignore for shutdown 
		}catch(Exception e) {
			applicationlogger.error("Exception in Consume Method:",e);
		}finally{
			applicationlogger.error("Closing consumer due to Exception/WakeUpException & hence committing the offset");
			consumer.commitSync(offsets);
			consumer.close();
			try {
				applicationlogger.debug("Shutting down the producer"+this.threadName+this.producer);
				this.producer.shutdown();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applicationlogger.error("Unable to close the producer"+this.threadName);
			}
		}

	}



	//Will be called when you bring down the consumer
	public void shutdown() {
		System.out.println("Shutdown is called for thread:"+this.threadName);
		consumer.wakeup();
	}




	@Override
	public void run() {
		// TODO Auto-generated method stub			
		this.logger=getLogger(loggingpath + "consumer-processing-" +Thread.currentThread().getName() + ".log", "consumer-processing-" +Thread.currentThread().getName(), loglevel);
		this.threadName=Thread.currentThread().getName();
		consume(logger);
	}


}
