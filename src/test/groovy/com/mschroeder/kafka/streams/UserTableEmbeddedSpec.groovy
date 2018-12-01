package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.User
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


    private static final long DEFAULT_TIMEOUT = 10 * 1000L

    static <K, V> void produceKeyValuesSynchronously(String topic, Collection<KeyValue<K, V>> records, Producer producer) {
        for (KeyValue<K, V> record : records) {
            Future<RecordMetadata> f = producer.send(new ProducerRecord<>(topic, record.key, record.value))
            f.get()
        }
        producer.flush()
        producer.close()
    }

    static <T> T waitUntilStoreIsQueryable(final String storeName,
                                              final QueryableStoreType<T> queryableStoreType,
                                              final KafkaStreams streams) {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType)
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(50)
            }
        }
    }



    /**
     * Asserts that the key-value store contains exactly the expected content and nothing more.
     *
     * @param store    the store to be validated
     * @param expected the expected contents of the store
     * @param <K>      the store's key type
     * @param <V>      the store's value type
     */
    static <K, V> void assertThatKeyValueStoreContains(ReadOnlyKeyValueStore<K, V> store, Map<K, V> expected) {
        TestUtils.waitForCondition(
                { expected.keySet().every { k ->  expected[k] == store.get(k) } },
                DEFAULT_TIMEOUT,
                "Expected values not found in KV store"
        )
    }

    /**
     * Asserts that the key-value store contains exactly the expected content and nothing more.
     *
     * @param store    the store to be validated
     * @param expected the expected contents of the store
     * @param <K>      the store's key type
     * @param <V>      the store's value type
     */
    static <K, V> void assertThatStoreContainsKeys(ReadOnlyKeyValueStore<K, V> store, List<K> expected)
            throws InterruptedException {
        TestUtils.waitForCondition(
                {expected.stream().every { k -> store.get(k) != null }},
                DEFAULT_TIMEOUT,
                "Expected keys not found in KV store"
        )
    }
}
