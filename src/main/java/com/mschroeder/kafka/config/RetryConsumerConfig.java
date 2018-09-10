package com.mschroeder.kafka.config;

import com.mschroeder.kafka.domain.ImportantData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.MANUAL;

@Configuration
public class RetryConsumerConfig {
	private final KafkaProperties kafkaProperties;

	@Autowired
	public RetryConsumerConfig(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}

	@Bean
	public RetryTemplate retryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();

		// retry 5 times, but only for KafkaException
		retryTemplate.setRetryPolicy(new SimpleRetryPolicy(5, singletonMap(KafkaException.class, true)));
		// exponentially backoff (default 100ms, multiplier 2)
		retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());

		return retryTemplate;
	}

	@Bean
	public ConsumerFactory<String, ImportantData> consumerFactory() {
		Map<String, Object> props = kafkaProperties.buildConsumerProperties();
		// override the deserializer prop to use JSON
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean("retryListenerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, ImportantData> retryListenerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, ImportantData> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setAckMode(MANUAL);
		factory.setConsumerFactory(consumerFactory());
		factory.setRetryTemplate(retryTemplate());

		return factory;
	}
}
