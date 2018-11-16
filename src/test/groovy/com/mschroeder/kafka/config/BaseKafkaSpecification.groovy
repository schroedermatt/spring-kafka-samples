package com.mschroeder.kafka.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import spock.lang.Specification

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@Import([MockBeanFactory, MockSchemaRegistryConfig])
@EmbeddedKafka(topics = ['${topics.retry-data}', '${topics.example-data}', 'user-updates'], controlledShutdown = true)
class BaseKafkaSpecification extends Specification {
    @Autowired
    protected static KafkaEmbedded kafkaEmbedded

    def cleanupSpec() {
        if (kafkaEmbedded) {
            kafkaEmbedded.after()
        }
    }

// alternative to @EmbeddedKafka
//    @ClassRule
//    public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(
//            1,
//            true,
//            'retry-topic',
//            'example-data-topic'
//    )
}
