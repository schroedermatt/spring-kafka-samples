package com.mschroeder.kafka.config;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class SerdeFactory {
	/**
	 * Generates a SpecifcAvroSerde with the provided props. This method will
	 * assume that the Serde is for the Value and not the Key.
	 * @param props avro props (see AbstractKafkaAvroSerDeConfig)
	 * @param <T> Type of AvroSerde
	 * @return SpecificAvroSerde
	 */
	public <T extends SpecificRecord> Serde<T> createSpecificSerde(Map<String, ?> props) {
		return createSpecificSerde(props, false, null);
	}

	/**
	 * Generates a SpecifcAvroSerde with the provided props. This method will
	 * assume that the Serde is for the Value and not the Key.
	 * @param props avro props (see AbstractKafkaAvroSerDeConfig)
	 * @param <T> Type of AvroSerde
	 * @return SpecificAvroSerde
	 */
	public <T extends SpecificRecord> Serde<T> createSpecificSerde(
			Map<String, ?> props, SchemaRegistryClient schemaRegistryClient) {
		return createSpecificSerde(props, false, schemaRegistryClient);
	}


	/**
	 * Generates a SpecifcAvroSerde with the provided props.
	 * @param props avro props (see AbstractKafkaAvroSerDeConfig)
	 * @param isKey whether or not the serde is for a key or value
	 * @param <T> Type of AvroSerde
	 * @return SpecificAvroSerde
	 */
	public <T extends SpecificRecord> Serde<T> createSpecificSerde(
			Map<String, ?> props, boolean isKey, SchemaRegistryClient schemaRegistryClient) {
		Serde<T> serde;
		if (schemaRegistryClient != null) {
			serde = new SpecificAvroSerde<>(schemaRegistryClient);
		} else {
			serde = new SpecificAvroSerde<>();
		}
		serde.configure(props, false);
		return serde;
	}
}
