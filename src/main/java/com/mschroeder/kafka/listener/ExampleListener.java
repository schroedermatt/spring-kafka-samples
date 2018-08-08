package com.mschroeder.kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ExampleListener {
	@KafkaListener(topics = "${topics.example-data}")
	public void listen(ConsumerRecord<String, String> record) {
		log.info("received: key={}, value={}", record.key(), record.value());
	}
}
