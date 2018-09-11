package com.mschroeder.kafka.listener;

import com.mschroeder.kafka.avro.AvroSampleData;
import com.mschroeder.kafka.domain.ImportantData;
import com.mschroeder.kafka.service.ImportantDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AvroListener {
	private final ImportantDataService dataService;

	public AvroListener(ImportantDataService importantDataService) {
		this.dataService = importantDataService;
	}

	@KafkaListener(topics = "${topics.example-data}", containerFactory = "avroListenerFactory")
	public void listen(ConsumerRecord<String, AvroSampleData> record, Acknowledgment acks) {
		log.info("received: key={}, value={}", record.key(), record.value());

		// convert avro data to important data
		AvroSampleData sampleData = record.value();
		ImportantData data = new ImportantData();
		data.setId(sampleData.getId());
		data.setName(sampleData.getName());
		data.setDescription("this is avro data");

		dataService.syncData(data);

		acks.acknowledge();
		log.info("message acknowledged.");
	}
}
