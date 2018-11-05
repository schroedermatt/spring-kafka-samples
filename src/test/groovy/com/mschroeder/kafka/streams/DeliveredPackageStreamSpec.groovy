package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.PackageEvent
import com.mschroeder.kafka.avro.User
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import spock.lang.Shared

class DeliveredPackageStreamSpec extends BaseStreamSpec {
	@Shared
	private Serde<User> userSerde

	@Shared
	private Serde<PackageEvent> packageEventSerde

	@Shared
	private KTable<String, User> usersKTable

	@Shared
	private ConsumerRecordFactory recordFactory

	private static Map SERDE_PROPS = [
			(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG): 'http://sr:8081',
			(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG): true
	]

	def setupSpec() {
		userSerde = new SpecificAvroSerde<>(mockSchemaRegistry)
		userSerde.configure(SERDE_PROPS, false)
		packageEventSerde = new SpecificAvroSerde<>(mockSchemaRegistry)
		packageEventSerde.configure(SERDE_PROPS, false)
	}

	@Override
	void configureTestDriver() {
		StreamsBuilder builder = new StreamsBuilder()

		// build aggregated transactions ktable
//		def ktable = builder.table(
//				"aggregate-ktable-changelog",
//				Materialized.<String, PackageTransactions, KeyValueStore<Bytes, byte[]>>
//						as(AggregatedTransactionsTable.KTABLE_NAME)
//						.withKeySerde(Serdes.String())
//						.withValueSerde(transactionsSerde)
//						.withCachingDisabled()
//						.withLoggingDisabled()
//		)

		def table = new UserTable()
		def ktable = table.usersKTable(userSerde, builder)

		new DeliveredPackageStream().deliveredPackage(
				ktable,
				packageEventSerde,
				userSerde,
				builder
		)

		testDriver = new TopologyTestDriver(builder.build(), defaultProps)

		recordFactory = new ConsumerRecordFactory<String, PackageEvent>(
				new StringSerializer(),
				packageEventSerde.serializer()
		)

		// prepopulate ktable via testDriver
		User user = User.newBuilder().setUserName("user-1").setEmail("user@email.com").setUserId(12).setNotificationsEnabled(true).build()
		KeyValueStore store = testDriver.getKeyValueStore(UserTable.KTABLE_NAME)
		store.put("123", user)
	}

	def 'delivered event comes through to the end topic'() {
		given:
		def event = PackageEvent
				.newBuilder()
				.setUserId(123)
				.setEventId(321)
				.setEventType("DELIVERED")
				.setPackageId(1234567)
				.build()
		def record = recordFactory.create("package-events", "123", event)

		when:
		testDriver.pipeInput(record)

		then: "a message is published to the delivered events topic"
		ProducerRecord<String, PackageEvent> recordFound = testDriver.readOutput(
				"delivered-package-events",
				new StringDeserializer(),
				packageEventSerde.deserializer()
		)

		recordFound
	}


	def 'non-delivered event does not come through to the end topic'() {
		given:
		def event = PackageEvent
				.newBuilder()
				.setUserId(123)
				.setEventId(321)
				.setEventType("OTHER")
				.setPackageId(1234567)
				.build()
		def record = recordFactory.create("package-events", "123", event)

		when:
		testDriver.pipeInput(record)

		then: "a message is published to the delivered events topic"
		ProducerRecord<String, PackageEvent> recordFound = testDriver.readOutput(
				"delivered-package-events",
				new StringDeserializer(),
				packageEventSerde.deserializer()
		)

		!recordFound
	}



	def 'non-matching key does not come through to the end topic'() {
		given:
		def event = PackageEvent
				.newBuilder()
				.setUserId(123)
				.setEventId(321)
				.setEventType("DELIVERED")
				.setPackageId(1234567)
				.build()
		def record = recordFactory.create("package-events", "321", event)

		when:
		testDriver.pipeInput(record)

		then: "a message is published to the delivered events topic"
		ProducerRecord<String, PackageEvent> recordFound = testDriver.readOutput(
				"delivered-package-events",
				new StringDeserializer(),
				packageEventSerde.deserializer()
		)

		!recordFound
	}
}
