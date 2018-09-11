package com.mschroeder.kafka.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.test.annotation.DirtiesContext
import spock.lang.Specification

@SpringBootTest
@EmbeddedKafka(topics = ['retry-topic', 'example-data-topic'])
class BaseKafkaSpecification extends Specification {
    @Autowired
    KafkaEmbedded kafkaEmbedded

    def cleanup() {
        kafkaEmbedded.after()
    }
}
