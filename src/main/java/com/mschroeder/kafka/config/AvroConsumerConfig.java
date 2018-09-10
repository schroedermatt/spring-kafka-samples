package com.mschroeder.kafka.config;

import com.mschroeder.kafka.avro.AvroSampleData;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

import static org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.MANUAL;

@Configuration
public class AvroConsumerConfig {
	private final KafkaProperties kafkaProperties;

	@Autowired
	public AvroConsumerConfig(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}


	@Bean
	public ConsumerFactory<String, AvroSampleData> avroConsumerFactory() {
		Map<String, Object> props = kafkaProperties.buildConsumerProperties();
		// override the deserializer prop to use Avro
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-listener");
//		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "avro-client");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean("avroListenerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, AvroSampleData> avroListenerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, AvroSampleData> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setAckMode(MANUAL);
		factory.setConsumerFactory(avroConsumerFactory());

		return factory;
	}
}
