package com.mschroeder.kafka.config

import com.mschroeder.kafka.listener.CountdownLatchRetryListener
import com.mschroeder.kafka.listener.RetryListener
import com.mschroeder.kafka.service.ImportantDataService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import spock.mock.DetachedMockFactory

@Configuration
class MockConsumerConfig {
    def mockFactory = new DetachedMockFactory()

    @Bean
    @Primary
    ImportantDataService importantDataServiceMock() {
        return mockFactory.Mock(ImportantDataService)
    }

    @Bean
    @Primary
    RetryListener retryListener() {
        return new CountdownLatchRetryListener(importantDataServiceMock(), 1)
    }
}
