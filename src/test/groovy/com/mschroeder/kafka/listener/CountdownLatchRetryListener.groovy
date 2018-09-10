package com.mschroeder.kafka.listener

import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment

import java.util.concurrent.CountDownLatch

class CountdownLatchRetryListener extends RetryListener {
    public final CountDownLatch latch

    CountdownLatchRetryListener(ImportantDataService service, int latchCount) {
        super(service)

        // configure CountDownLatch for testing
        this.latch = new CountDownLatch(latchCount)
    }

    @KafkaListener(topics = '${topics.retry-data}', containerFactory = "retryListenerFactory")
    void listen(ConsumerRecord<String, ImportantData> record, Acknowledgment acks) {
        super.listen(record, acks)

        latch.countDown()
    }
}
