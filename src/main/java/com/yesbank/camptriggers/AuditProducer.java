package com.yesbank.camptriggers;

import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import com.google.gson.JsonObject;

public class AuditProducer {

	private Properties properties;
	private Producer<String, String> producer;

	public AuditProducer(String bootstrapServers,String topicName,String serviceName) throws Exception {
		try {
			properties=new Properties();
			properties.setProperty("bootstrap.servers",bootstrapServers);
			properties.setProperty("acks","1");
			properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
			properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
			properties.setProperty("retries","0");
			properties.setProperty("batch.size","16384");
			properties.setProperty("linger.ms","1");
			properties.setProperty("buffer.memory","33554432");	
			properties.setProperty("security.protocol","SASL_PLAINTEXT");
			properties.setProperty("sasl.kerberos.service.name",serviceName);
			this.producer=new KafkaProducer<String,String>(properties);
		}catch(Exception e) {
			throw new Exception("Unable to intialize the producer"+e.getMessage());

		}

	}	





	//produce the data to kafka topic
	public void produce(ProducerRecord<String, String> record) throws Exception {

		producer.send(record);
	}

	ProducerRecord<String, String> auditmessage( JsonObject inputPayload,int statuscode,String message,Long responseId,String auditTopicName) {
		// TODO Auto-generated method stub
		JsonObject status = new JsonObject();
		if(inputPayload.get("customer_id")!=null)
			status.addProperty("customerID",inputPayload.get("customer_id").getAsLong());
		if(inputPayload.get("kafkaevent_id")!=null)
			status.addProperty("kafkaMessageID", inputPayload.get("kafkaevent_id").getAsString());
		if(inputPayload.get("kafkaevent_key")!=null)
			status.addProperty("kafkaEvent", inputPayload.get("kafkaevent_key").getAsString());
		if(inputPayload.get("adobeevent_id")!=null)
			status.addProperty("adobeEvent", inputPayload.get("adobeevent_id").getAsString());
		JsonObject responseStatus=new JsonObject();
		responseStatus.addProperty("statusCode", statuscode);
		responseStatus.addProperty("message", message);
		responseStatus.addProperty("responseid", responseId);
		responseStatus.addProperty("loggingTime",System.currentTimeMillis());
		status.addProperty("status",responseStatus.toString());
		return new ProducerRecord<String,String>(auditTopicName, status.toString());

	}

	public void shutdown() throws Exception{
		producer.close();
	}

}
