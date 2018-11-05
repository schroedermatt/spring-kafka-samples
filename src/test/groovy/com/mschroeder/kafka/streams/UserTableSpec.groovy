package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.User
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import spock.lang.Shared

class UserTableSpec extends BaseStreamSpec {
	@Shared
	private Serde<User> userSerde

	@Shared
	private ConsumerRecordFactory recordFactory

	private static Map SERDE_PROPS = [
			(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG): 'http://sr:8081',
			(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG): true
	]

	def setupSpec() {
		userSerde = new SpecificAvroSerde<>(mockSchemaRegistry)
		userSerde.configure(SERDE_PROPS, false)
		recordFactory = new ConsumerRecordFactory<String, User>(
                new StringSerializer(),
				userSerde.serializer()
		)
	}

	@Override
	void configureTestDriver() {
		// configure StreamsBuilder by reference
		StreamsBuilder builder = new StreamsBuilder()
		new UserTable().usersKTable(userSerde, builder)

		testDriver = new TopologyTestDriver(builder.build(), defaultProps)
	}

	def 'single user message is stored in ktable'() {
		given: "a user avro message"
			User user = User.newBuilder()
				.setUserId(123)
				.setEmail('john.doe@email.com')
				.setNotificationsEnabled(false)
				.setUserName('john')
				.build()

			ConsumerRecord consumerRecord = recordFactory.create(
					UserTable.SOURCE_TOPIC,
					user.getUserId() as String,
					user
			)

		when: "supplying it to the test driver to process"
			testDriver.pipeInput(consumerRecord)

		then: "it is stored in the KTable"
			def store = testDriver.getKeyValueStore(UserTable.KTABLE_NAME)
			User result = store.get(user.getUserId() as String) as User

			result
            result == user
	}

    def 'existing user is updated in ktable'() {
        given: "a user avro message"
        User user = User.newBuilder()
                .setUserId(123)
                .setEmail('john.doe@email.com')
                .setNotificationsEnabled(false)
                .setUserName('john')
                .build()

        def store = testDriver.getKeyValueStore(UserTable.KTABLE_NAME)
        store.put("123", user)

        user.setNotificationsEnabled(true)
        ConsumerRecord consumerRecord = recordFactory.create(
                UserTable.SOURCE_TOPIC,
                user.getUserId() as String,
                user
        )

        when: "supplying it to the test driver to process"
        testDriver.pipeInput(consumerRecord)

        then: "it is stored in the KTable"
        User result = store.get(user.getUserId() as String) as User

        result
        result.getNotificationsEnabled()
    }
}
