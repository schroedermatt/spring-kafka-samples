package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.Notification
import com.mschroeder.kafka.avro.PackageEvent
import com.mschroeder.kafka.avro.User
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import spock.lang.Shared

class DeliveryNotificationStreamSpec extends BaseStreamSpec {
	@Shared
	private Serde<User> userSerde

	@Shared
	private Serde<PackageEvent> packageEventSerde

	@Shared
	private Serde<Notification> notificationSerde

	@Shared
	private ConsumerRecordFactory recordFactory

	private static Map SERDE_PROPS = [
			(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG): 'http://sr:8081',
			(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG): true,
			(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS): true
	]

	def setupSpec() {
		userSerde = new SpecificAvroSerde<>(mockSchemaRegistry)
		userSerde.configure(SERDE_PROPS, false)
		packageEventSerde = new SpecificAvroSerde<>(mockSchemaRegistry)
		packageEventSerde.configure(SERDE_PROPS, false)
		notificationSerde = new SpecificAvroSerde<>(mockSchemaRegistry)
		notificationSerde.configure(SERDE_PROPS, false)
	}

	@Override
	void configureTestDriver() {
		StreamsBuilder builder = new StreamsBuilder()

		def table = new UserTable()
		def ktable = table.usersKTable(userSerde, builder)

		new DeliveryNotificationStream().deliveredPackage(
				ktable,
				packageEventSerde,
				userSerde,
				notificationSerde,
				builder
		)

		testDriver = new TopologyTestDriver(builder.build(), defaultProps)

		recordFactory = new ConsumerRecordFactory<Integer, PackageEvent>(
				new IntegerSerializer(),
				packageEventSerde.serializer()
		)

		// prepopulate ktable via testDriver
		User user = User.newBuilder().setUserName("user-1").setEmail("user@email.com").setUserId(123).setNotificationsEnabled(true).build()
		KeyValueStore store = testDriver.getKeyValueStore(UserTable.KTABLE_NAME)
		store.put(123, user)
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
		def record = recordFactory.create("package-events", 123456, event)

		when:
		testDriver.pipeInput(record)

		then: "a message is published to the delivered events topic"
		ProducerRecord<Integer, Notification> recordFound = testDriver.readOutput(
				"delivery-notifications",
				new IntegerDeserializer(),
				notificationSerde.deserializer()
		)

		recordFound
		recordFound.value().email == "user@email.com"
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
		def record = recordFactory.create("package-events", 123, event)

		when:
		testDriver.pipeInput(record)

		then: "a message is published to the delivered events topic"
		ProducerRecord<Integer, Notification> recordFound = testDriver.readOutput(
				"delivery-notifications",
				new IntegerDeserializer(),
				notificationSerde.deserializer()
		)

		!recordFound
	}

	def 'non-matching key does not come through to the end topic'() {
		given:
		def event = PackageEvent
				.newBuilder()
				.setUserId(321)
				.setEventId(321)
				.setEventType("DELIVERED")
				.setPackageId(1234567)
				.build()
		def record = recordFactory.create("package-events", 1234, event)

		when:
		testDriver.pipeInput(record)

		then: "a message is published to the delivered events topic"
		ProducerRecord<Integer, Notification> recordFound = testDriver.readOutput(
				"delivery-notifications",
				new IntegerDeserializer(),
				notificationSerde.deserializer()
		)

		!recordFound
	}
}
