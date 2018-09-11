package com.mschroeder.kafka.listener;

import com.mschroeder.kafka.domain.ImportantData;
import com.mschroeder.kafka.service.ImportantDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RetryListener {
	private final ImportantDataService dataService;
	public RetryListener(ImportantDataService service) {
		this.dataService = service;
	}

	@KafkaListener(topics = "${topics.retry-data}", containerFactory = "kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<String, ImportantData> record, Acknowledgment acks) {
		log.info("received: key={}, value={}", record.key(), record.value());
		dataService.syncData(record.value());
		acks.acknowledge();
		log.info("message acknowledged.");
	}
}
