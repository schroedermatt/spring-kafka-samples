package com.mschroeder.kafka.listener;

import com.mschroeder.kafka.domain.ImportantData;
import com.mschroeder.kafka.service.ImportantDataServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RetryListener {
	private final ImportantDataServiceImpl importantDataServiceImpl;
	public RetryListener(ImportantDataServiceImpl service) {
		this.importantDataServiceImpl = service;
	}

	@KafkaListener(topics = "${topics.retry-data}", containerFactory = "retryListenerFactory")
	public void listen(ConsumerRecord<String, ImportantData> record, Acknowledgment acks) {
		log.info("received: key={}, value={}", record.key(), record.value());
		importantDataServiceImpl.syncData(record.value());
		acks.acknowledge();
		log.info("message acknowledged.");
	}
}
