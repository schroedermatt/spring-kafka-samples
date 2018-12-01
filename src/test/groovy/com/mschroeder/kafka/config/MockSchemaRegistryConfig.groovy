package com.mschroeder.kafka.config

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

@Configuration
class MockSchemaRegistryConfig {
	private KafkaProperties props

	MockSchemaRegistryConfig(KafkaProperties kafkaProperties) {
		props = kafkaProperties
	}

	/**
	 * Mock schema registry bean used by Kafka Avro Serde since
	 * the @EmbeddedKafka setup doesn't include a schema registry.
	 * @return MockSchemaRegistryClient instance
	 */
	@Bean
	@Primary
	SchemaRegistryClient schemaRegistryClient() {
		new MockSchemaRegistryClient()
	}

	/**
	 * KafkaAvroSerializer that uses a MockSchemaRegistryClient
	 * @return KafkaAvroSerializer instance
	 */
	@Bean
	KafkaAvroSerializer kafkaAvroSerializer() {
		Map props = props.buildConsumerProperties()
		props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true)

		new KafkaAvroSerializer(schemaRegistryClient(), props)
	}

	/**
	 * KafkaAvroDeserializer that uses a MockSchemaRegistryClient
	 * @return KafkaAvroDeserializer instance
	 */
	@Bean
	KafkaAvroDeserializer kafkaAvroDeserializer() {
		Map props = props.buildConsumerProperties()
		props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true)

		new KafkaAvroDeserializer(schemaRegistryClient(), props)
    }
}
