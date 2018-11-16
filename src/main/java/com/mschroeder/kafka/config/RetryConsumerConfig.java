package com.mschroeder.kafka.config;

import com.mschroeder.kafka.domain.ImportantData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Map;

import static java.util.Collections.singletonMap;

@Slf4j
@Configuration
public class RetryConsumerConfig {
	private final KafkaProperties kafkaProperties;

	public RetryConsumerConfig(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}

	@Bean
	public RetryTemplate retryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();

		// retry 5 times, but only for KafkaException
		retryTemplate.setRetryPolicy(new SimpleRetryPolicy(5, singletonMap(KafkaException.class, true)));

		// fixed backoff
		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		policy.setBackOffPeriod(50); // milliseconds

		retryTemplate.setBackOffPolicy(policy);

		return retryTemplate;
	}

	@Bean
	public ConsumerFactory<String, ImportantData> jsonConsumerFactory() {
		Map<String, Object> props = kafkaProperties.buildConsumerProperties();
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-listener");
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
//		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "json-client");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean("kafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, ImportantData> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, ImportantData> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
		factory.setConsumerFactory(jsonConsumerFactory());

		factory.setRetryTemplate(retryTemplate());

		// create simple recovery callback (this gets executed once retry policy limits have been exceeded)
		factory.setRecoveryCallback(context -> {
			log.error("RetryPolicy limit has been exceeded!");
			return null;
		});

		return factory;
	}
}
