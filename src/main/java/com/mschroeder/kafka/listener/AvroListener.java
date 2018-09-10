package com.mschroeder.kafka.listener;

import com.mschroeder.kafka.avro.AvroSampleData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AvroListener {
	@KafkaListener(topics = "${topics.example-data}", containerFactory = "retryListenerFactory")
	public void listen(ConsumerRecord<String, AvroSampleData> record, Acknowledgment acks) {
		log.info("received: key={}, value={}", record.key(), record.value());
		acks.acknowledge();
		log.info("message acknowledged.");
	}
}
