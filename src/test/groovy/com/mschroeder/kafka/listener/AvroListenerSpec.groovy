package com.mschroeder.kafka.listener

import com.mschroeder.kafka.avro.AvroSampleData
import com.mschroeder.kafka.config.BaseKafkaSpecification
import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataService
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class AvroListenerSpec extends BaseKafkaSpecification {
    private CountDownLatch latch

    @Value('${topics.example-data}')
    private String topic

    @Autowired
    private KafkaTemplate<String, AvroSampleData> kafkaTemplate

    @Autowired
    private ImportantDataService mockDataService

    def 'listener receives specific avro message from embedded broker'() {
        given:
        latch = new CountDownLatch(1)
        mockDataService.syncData(_ as ImportantData) >> { latch.countDown() }

        when:
        sendMessage()

        then:
        latch.await(2, TimeUnit.SECONDS)
    }

    /** PRIVATE TEST HELPERS **/

    private void sendMessage() {
        def message = AvroSampleData
                .newBuilder()
                .setId(1)
                .setName("testing")
                .setDate(DateTime.now())
                .build()

        kafkaTemplate.send(topic, "123", message)
        kafkaTemplate.flush()
    }
}
