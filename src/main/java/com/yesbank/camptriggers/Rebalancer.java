package com.yesbank.camptriggers;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class Rebalancer implements ConsumerRebalanceListener {



	private KafkaConsumer<String, String> consumer;
	private Map<TopicPartition, OffsetAndMetadata> offsets;
	private Logger log;


	public Rebalancer(KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> offsets,Logger log) {
		// TODO Auto-generated constructor stub
		this.consumer=consumer;
		this.offsets=offsets;
		this.log=log;

	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		log.error("Rebalancing has triggered,onPartitionsRevoked function has been called at "+System.currentTimeMillis());
		log.error("Commiting the offsets"+":"+offsets);
		consumer.commitSync(offsets);
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

	}

}
