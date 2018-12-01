package com.mschroeder.kafka.config

import com.mschroeder.kafka.avro.AvroSampleData
import com.mschroeder.kafka.avro.Notification
import com.mschroeder.kafka.avro.PackageEvent
import com.mschroeder.kafka.avro.User
import com.mschroeder.kafka.util.IntegrationTestUtil
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer

@Configuration
class ClientConfig {
    private KafkaProperties props
    private KafkaAvroSerializer avroSerializer
    private KafkaAvroDeserializer avroDeserializer
    private Serializer<String> stringSerializer = new StringSerializer()
    private Deserializer<String> stringDeserializer = new StringDeserializer()
    private Serializer<Integer> intSerializer = new IntegerSerializer()
    private Deserializer<Integer> integerDeserializer = new IntegerDeserializer()

    ClientConfig(KafkaProperties properties,
                 KafkaAvroSerializer serializer,
                 KafkaAvroDeserializer deserializer) {
        this.props = properties
        this.avroSerializer = serializer
        this.avroDeserializer = deserializer
    }

    ProducerFactory producerFactory(Serializer keySer, Serializer valSer) {
        producerFactory(props.buildProducerProperties(), keySer, valSer)
    }

    ProducerFactory producerFactory(Map props, Serializer keySer, Serializer valSer) {
        new DefaultKafkaProducerFactory(
                props,
                keySer,
                valSer
        )
    }

    @Bean
    KafkaTemplate<String, AvroSampleData> avroKafkaTemplate() {
        new KafkaTemplate<>(producerFactory(stringSerializer, avroSerializer))
    }

    @Bean
    Producer<Integer, User> userProducer() {
        producerFactory(intSerializer, avroSerializer).createProducer()
    }

    @Bean
    Producer<Integer, PackageEvent> packageEventProducer() {
        producerFactory(intSerializer, avroSerializer).createProducer()
    }


    // CONSUMER CONFIG

    @Bean
    ConsumerFactory consumerFactory() {
        new DefaultKafkaConsumerFactory<>(
                props.buildConsumerProperties(),
                integerDeserializer,
                avroDeserializer
        )
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
                stringDeserializer,
                avroDeserializer
        )
    }

    @Bean("userAvroConsumerFactory")
    ConsumerFactory<Integer, User> userAvroConsumerFactory() {
        def props = props.buildConsumerProperties()
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-user-listener")

        new DefaultKafkaConsumerFactory(
                props,
                integerDeserializer,
                avroDeserializer
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
