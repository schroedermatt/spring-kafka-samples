package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.User
import com.mschroeder.kafka.util.IntegrationTestUtil
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreType
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.test.TestUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.StreamsBuilderFactoryBean
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import spock.lang.Specification

import java.util.concurrent.Future

import static com.mschroeder.kafka.util.IntegrationTestUtil.assertThatStoreContainsKeys
import static com.mschroeder.kafka.util.IntegrationTestUtil.produceKeyValuesSynchronously
import static com.mschroeder.kafka.util.IntegrationTestUtil.waitUntilStoreIsQueryable

@SpringBootTest
@ActiveProfiles('test')
@EmbeddedKafka(topics = ['user-updates', 'package-events', 'delivery-notifications'])
class UserTableEmbeddedSpec extends Specification {
    @Autowired
    StreamsBuilderFactoryBean factoryBean

    @Autowired
    Producer<Integer, User> userProducer

    def 'user message is stored in ktable'() {
        given: 'a user message'
        Integer userId = 123
        User user = User.newBuilder()
                .setUserId(userId)
                .setEmail('john.doe@email.com')
                .setNotificationsEnabled(false)
                .setUserName('john')
                .build()
        KeyValue record = new KeyValue<Integer, User>(userId, user)

        when: 'publishing them to the user-updates topic'
        produceKeyValuesSynchronously(UserTable.SOURCE_TOPIC, [record], userProducer)

        then: 'the user should be stored into the ktable'
        ReadOnlyKeyValueStore<Integer, User> store = waitUntilStoreIsQueryable(
                UserTable.KTABLE_NAME,
                QueryableStoreTypes.<Integer, User> keyValueStore(),
                factoryBean.kafkaStreams
        )

        assertThatStoreContainsKeys(store, [userId])
    }
}
