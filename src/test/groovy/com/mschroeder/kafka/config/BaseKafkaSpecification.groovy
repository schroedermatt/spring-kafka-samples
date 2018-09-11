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
@EmbeddedKafka(topics = ['retry-topic', 'example-data-topic'])
class BaseKafkaSpecification extends Specification {
    @Autowired
    protected static KafkaEmbedded kafkaEmbedded

    def setupSpec() {
        if (kafkaEmbedded) {
            kafkaEmbedded.after()
            kafkaEmbedded.before()
        }
    }

    def cleanupSpec() {
        // teardown the embedded kafka instance if it's still around
        if (kafkaEmbedded) {
            kafkaEmbedded.after()
        }
    }
}
