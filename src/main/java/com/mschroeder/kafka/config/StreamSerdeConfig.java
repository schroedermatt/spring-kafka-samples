package com.mschroeder.kafka.config;

import com.mschroeder.kafka.avro.Notification;
import com.mschroeder.kafka.avro.PackageEvent;
import com.mschroeder.kafka.avro.User;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class StreamSerdeConfig {
	@Autowired
	private SerdeFactory serdeFactory;

	@Autowired(required = false)
	private SchemaRegistryClient schemaRegistryClient;

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
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

		return new StreamsConfig(props);
	}

	@Bean
	Serde<PackageEvent> packageEventSerde(KafkaProperties props) {
		final Map<String, Object> serdeProps = props.buildConsumerProperties();
		return serdeFactory.createSpecificSerde(serdeProps, schemaRegistryClient);
	}

	@Bean
	Serde<User> userSerde(KafkaProperties props) {
		final Map<String, Object> serdeProps = props.buildConsumerProperties();
		return serdeFactory.createSpecificSerde(serdeProps, schemaRegistryClient);
	}

	@Bean
	Serde<Notification> notificationSerde(KafkaProperties props) {
		final Map<String, Object> serdeProps = props.buildConsumerProperties();
		return serdeFactory.createSpecificSerde(serdeProps, schemaRegistryClient);
	}
}