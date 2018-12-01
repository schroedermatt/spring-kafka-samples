package com.mschroeder.kafka.config

import com.mschroeder.kafka.avro.AvroSampleData
import com.mschroeder.kafka.avro.User
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.AbstractMessageListenerContainer

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG

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

	/**
	 * Configures the kafka producer factory to use the overridden
	 * KafkaAvroDeserializer so that the MockSchemaRegistryClient
	 * is used rather than trying to reach out via HTTP to a schema registry
	 * @param props KafkaProperties configured in application.yml
	 * @return DefaultKafkaProducerFactory instance
	 */
	ProducerFactory producerFactory() {
		new DefaultKafkaProducerFactory(
				props.buildProducerProperties(),
				new StringSerializer(),
				kafkaAvroSerializer()
		)
	}

	@Bean
	KafkaTemplate<String, AvroSampleData> avroKafkaTemplate() {
		new KafkaTemplate<>(producerFactory())
	}


	/**
	 * Configures the kafka producer factory to use the overridden
	 * KafkaAvroDeserializer so that the MockSchemaRegistryClient
	 * is used rather than trying to reach out via HTTP to a schema registry
	 * @param props KafkaProperties configured in application.yml
	 * @return DefaultKafkaProducerFactory instance
	 */
	ProducerFactory intProducerFactory() {
		new DefaultKafkaProducerFactory(
				props.buildProducerProperties(),
				new IntegerSerializer(),
				kafkaAvroSerializer()
		)
	}

	@Bean
	Producer<Integer, User> userProducer() {
		intProducerFactory().createProducer()
	}

	@Bean
	Properties producerProps() {
		Map props = props.buildProducerProperties()
		props[KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer
		props[VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer

		props as Properties
	}

	/**
	 * Configures the kafka consumer factory to use the overridden
	 * KafkaAvroSerializer so that the MockSchemaRegistryClient
	 * is used rather than trying to reach out via HTTP to a schema registry
	 * @param props KafkaProperties configured in application.yml
	 * @return DefaultKafkaConsumerFactory instance
	 */
	@Bean("avroConsumerFactory")
	ConsumerFactory<String, AvroSampleData> avroConsumerFactory() {
		def props = props.buildConsumerProperties()
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-listener")

		new DefaultKafkaConsumerFactory(
				props,
				new StringDeserializer(),
				kafkaAvroDeserializer()
		)
	}

	@Bean("userAvroConsumerFactory")
	ConsumerFactory<Integer, User> userAvroConsumerFactory() {
		def props = props.buildConsumerProperties()
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-user-listener")

		new DefaultKafkaConsumerFactory(
				props,
				new IntegerDeserializer(),
				kafkaAvroDeserializer()
		)
	}

	/**
	 * Configure the ListenerContainerFactory to use the overridden
	 * consumer factory so that the MockSchemaRegistryClient is used
	 * under the covers by all consumers when deserializing Avro data.
	 * @return ConcurrentKafkaListenerContainerFactory instance
	 */
	@Bean("avroListenerFactory")
	ConcurrentKafkaListenerContainerFactory<String, AvroSampleData> avroListenerFactory() {
		ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory()
		factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL)
		factory.setConsumerFactory(avroConsumerFactory())
		return factory
	}
}
