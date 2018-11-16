package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.User
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import spock.lang.Specification

class UserTableSpec extends Specification {
    MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient()
    Serde<User> userSerde = new SpecificAvroSerde<>(mockClient)
    ConsumerRecordFactory recordFactory
    TopologyTestDriver testDriver

    def setup() {
        // configure the message value serde
        Map serdeProps = [
                (AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG): 'http://sr:8081',
                (KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG): true,
                (AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS)     : true
        ]
        userSerde.configure(serdeProps, false)

        recordFactory = new ConsumerRecordFactory<Integer, User>(
                new IntegerSerializer(),
                userSerde.serializer()
        )

        // configure StreamsBuilder by reference
        StreamsBuilder builder = new StreamsBuilder()
        new UserTable().usersKTable(userSerde, builder)

        Properties streamProps = [
                (StreamsConfig.APPLICATION_ID_CONFIG)   : 'user-ktable-specification',
                (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG): 'broker:9091'
        ]

        testDriver = new TopologyTestDriver(builder.build(), streamProps)
    }

    def cleanup() {
        testDriver.close()
    }

    def 'user message is stored in ktable'() {
        given: "a user avro message"
        User user = User.newBuilder()
                .setUserId(123)
                .setEmail('john.doe@email.com')
                .setNotificationsEnabled(false)
                .setUserName('john')
                .build()

        ConsumerRecord consumerRecord = recordFactory.create(
                UserTable.SOURCE_TOPIC,
                user.getUserId(),
                user
        )

        when: "supplying it to the test driver to process"
        testDriver.pipeInput(consumerRecord)

        then: "it is stored in the KTable"
        KeyValueStore store = testDriver.getKeyValueStore(UserTable.KTABLE_NAME)
        User result = store.get(user.getUserId()) as User

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
        KeyValueStore store = testDriver.getKeyValueStore(UserTable.KTABLE_NAME)
        store.put(user.getUserId(), user)

        when:
        user.setNotificationsEnabled(true)
        ConsumerRecord consumerRecord = recordFactory.create(
                UserTable.SOURCE_TOPIC,
                user.getUserId(),
                user
        )

        and: "supplying it to the test driver to process"
        testDriver.pipeInput(consumerRecord)

        then: "it is stored in the KTable"
        User result = store.get(user.getUserId()) as User

        result
        result.getNotificationsEnabled()
    }
}
