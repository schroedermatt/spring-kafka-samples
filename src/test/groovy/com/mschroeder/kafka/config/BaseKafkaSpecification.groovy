package com.mschroeder.kafka.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.rule.KafkaEmbedded
import spock.lang.Specification

@SpringBootTest
@Import([MockBeanFactory])
@EmbeddedKafka(topics = ['retry-topic', 'example-data-topic'])
class BaseKafkaSpecification extends Specification {
    @Autowired
    KafkaEmbedded kafkaEmbedded

    def cleanup() {
        kafkaEmbedded.after()
    }
}
