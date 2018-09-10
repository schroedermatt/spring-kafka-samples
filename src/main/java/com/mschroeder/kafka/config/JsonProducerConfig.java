package com.mschroeder.kafka.config;

import com.mschroeder.kafka.domain.ImportantData;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class JsonProducerConfig {
	private final KafkaProperties kafkaProperties;

	public JsonProducerConfig(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}

	@Bean
	public ProducerFactory<String, ImportantData> importantDataProducerFactory() {
		Map<String, Object> props = kafkaProperties.buildProducerProperties();
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(props);
	}

	@Bean
	public KafkaTemplate<String, ImportantData> importDataKafkaTemplate() {
		return new KafkaTemplate<>(importantDataProducerFactory());
	}
}
