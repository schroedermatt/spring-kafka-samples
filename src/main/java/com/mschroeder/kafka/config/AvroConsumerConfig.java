package com.mschroeder.kafka.config;

import com.mschroeder.kafka.avro.AvroSampleData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import static org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.MANUAL;

@Configuration
public class AvroConsumerConfig {
	private final KafkaProperties kafkaProperties;

	@Autowired
	public AvroConsumerConfig(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}


	@Bean
	public ConsumerFactory<String, AvroSampleData> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
	}

	@Bean("avroListenerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, AvroSampleData> avroListenerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, AvroSampleData> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setAckMode(MANUAL);
		factory.setConsumerFactory(consumerFactory());

		return factory;
	}
}
