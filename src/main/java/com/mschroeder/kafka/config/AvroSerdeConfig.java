package com.mschroeder.kafka.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class AvroSerdeConfig {
	@Bean
	KafkaAvroSerializer kafkaAvroSerializer(KafkaProperties props) {
		KafkaAvroSerializer ser = new KafkaAvroSerializer();
		ser.configure(props.buildConsumerProperties(), false);
		return ser;
	}

	@Bean
	KafkaAvroDeserializer kafkaAvroDeserializer(KafkaProperties props) {
		KafkaAvroDeserializer de = new KafkaAvroDeserializer();
		de.configure(props.buildProducerProperties(), false);
		return de;
	}
}
