package com.mschroeder.kafka.listener

import com.mschroeder.kafka.avro.AvroSampleData
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.rule.KafkaEmbedded
import spock.lang.Specification

@SpringBootTest
@EmbeddedKafka(topics = '${topics.sample-data}')
class AvroListenerSpec extends Specification {
    @Value('${topics.sample-data}')
    private String topic

    @Autowired
    private KafkaTemplate<String, AvroSampleData> kafkaTemplate

    @Autowired
    private KafkaEmbedded kafkaEmbedded

    def cleanup() {
        kafkaEmbedded.after()
    }

    def 'can publish avro message to embedded broker'() {
        given:
        def message = AvroSampleData
                .newBuilder()
                .setId(1)
                .setName("testing")
                .setDate(DateTime.now())
                .build()
        when:
        kafkaTemplate.send(topic, "123", message)
        kafkaTemplate.flush()

        then:
        true
    }
}
