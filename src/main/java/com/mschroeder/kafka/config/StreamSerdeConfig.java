package com.mschroeder.kafka.config;

import com.mschroeder.kafka.avro.PackageEvent;
import com.mschroeder.kafka.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class StreamSerdeConfig {
	/**
	 * This config will be used as the default for ALL streams.
	 * Streams should override individual props as needed.
	 * @param kafkaProps kafka properties pulled from application.yaml
	 * @return defaultKafkaStreamsConfig instance
	 */
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig streamsConfig(KafkaProperties kafkaProps) {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getBootstrapServers());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");

		return new StreamsConfig(props);
	}

	@Bean
	Serde<PackageEvent> packageEventSerde(SerdeFactory factory, KafkaProperties props) {
		final Map<String, Object> serdeProps = props.buildConsumerProperties();
		return factory.createSpecificSerde(serdeProps);
	}

	@Bean
	Serde<User> userSerde(SerdeFactory factory, KafkaProperties props) {
		final Map<String, Object> serdeProps = props.buildConsumerProperties();
		return factory.createSpecificSerde(serdeProps);
	}
}