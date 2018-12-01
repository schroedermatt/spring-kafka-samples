package com.mschroeder.kafka.util

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreType
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.test.TestUtils
import java.util.concurrent.Future

class IntegrationTestUtil {
    private static final long DEFAULT_TIMEOUT = 10 * 1000L
    private static final int UNLIMITED_MESSAGES = -1

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


    /**
     * Returns up to `maxMessages` message-values from the topic.
     */
    static <K, V> List<V> readValues(String topic, Consumer consumer, int maxMessages) {
        List<KeyValue<K, V>> kvs = readKeyValues(topic, consumer, maxMessages)
        return kvs.collect { it.value }
    }

    /**
     * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is
     * reached.
     */
    static <K, V> List<KeyValue<K, V>> readKeyValues(String topic,  Consumer consumer) {
        readKeyValues(topic, consumer, UNLIMITED_MESSAGES)
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
     * already configured in the consumer).
     */
    static <K, V> List<KeyValue<K, V>> readKeyValues(String topic, Consumer consumer, int maxMessages) {
        consumer.subscribe([topic])
        int pollIntervalMs = 100
        int maxTotalPollTimeMs = 2000
        int totalPollTimeMs = 0
        List<KeyValue<K, V>> consumedValues = []
        while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs
            ConsumerRecords<K, V> records = consumer.poll(pollIntervalMs)
            for (ConsumerRecord<K, V> record : records) {
                consumedValues << new KeyValue<>(record.key(), record.value())
            }
        }
        consumer.close()
        return consumedValues
    }

    private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages
    }

}
