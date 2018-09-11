package com.mschroeder.kafka.listener

import com.mschroeder.kafka.avro.AvroSampleData
import com.mschroeder.kafka.config.BaseKafkaSpecification
import com.mschroeder.kafka.config.MockSerdeConfig
import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataService
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Import([MockSerdeConfig])
class AvroListenerSpec extends BaseKafkaSpecification {
    private CountDownLatch latch

    @Value('${topics.example-data}')
    private String topic

    @Autowired
    private KafkaTemplate<String, AvroSampleData> kafkaTemplate

    @Autowired
    private ImportantDataService mockDataService

    def 'can publish avro message to embedded broker'() {
        given:
        latch = new CountDownLatch(1)

        def message = AvroSampleData
                .newBuilder()
                .setId(1)
                .setName("testing")
                .setDate(DateTime.now())
                .build()

        mockDataService.syncData(_ as ImportantData) >> { latch.countDown() }

        when:
        kafkaTemplate.send(topic, "123", message)
        kafkaTemplate.flush()

        then:
        latch.await(5, TimeUnit.SECONDS)
    }
}
