package com.mschroeder.kafka.config

import com.mschroeder.kafka.service.ImportantDataService
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.support.Acknowledgment
import spock.mock.DetachedMockFactory

@TestConfiguration
class MockBeanFactory {
    def mockFactory = new DetachedMockFactory()

    @Bean
    @Primary
    ImportantDataService importantDataServiceMock() {
        mockFactory.Mock(ImportantDataService)
    }

// use acks to run the CountdownLatch?
//    @Bean
//    @Primary
//    Acknowledgment mockAcknowledgement() {
//        mockFactory.Mock(Acknowledgment)
//    }
}
