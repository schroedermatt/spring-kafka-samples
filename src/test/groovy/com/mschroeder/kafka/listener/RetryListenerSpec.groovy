package com.mschroeder.kafka.listener

import com.mschroeder.kafka.config.BaseKafkaSpecification
import com.mschroeder.kafka.config.MockBeanFactory
import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataService
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.annotation.DirtiesContext

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Slf4j
@Import([MockBeanFactory])
class RetryListenerSpec extends BaseKafkaSpecification {
	// used for keeping track of mock calls to dataService
	private int mockExecutions
	// used to keep test waiting while kafka consumer runs
	private CountDownLatch latch
	// sample important data for tests
	private ImportantData data = new ImportantData(
			id: 1,
			name: 'testing',
			description: 'this is just a retry, this is just a retry'
	)

	@Autowired
	KafkaTemplate<String, ImportantData> kafkaTemplate

	@Autowired
	ImportantDataService mockDataService

	def setup() {
		mockExecutions = 0
	}

	def cleanup() {
		mockExecutions = 0
	}

	def fail() {
		log.error("FAILING ON EXECUTION #{}", mockExecutions)
		throw new KafkaException("oops!")
	}

	def succeed() {
		log.info("SUCCEEDING ON EXECUTION #{}", mockExecutions)
		latch.countDown()
	}

	def 'RetryTemplate: message will succeed after failing 3 times'() {
		given: 'the expectation to sync the data once'
		latch = new CountDownLatch(1)

		// fail 3 times and then succeed
		mockDataService.syncData(_ as ImportantData) >> {
			mockExecutions++

			if (mockExecutions < 4) {
				fail()
			}

			succeed()
		}

		when: 'a message is published'
		kafkaTemplate.send('retry-topic', "123", data)
		kafkaTemplate.flush()

		then: 'the latch will countdown within 10 seconds'
		latch.await(5, TimeUnit.SECONDS)
		mockExecutions == 4
	}

	def 'RetryTemplate: message will fail after limit of 5 attempts'() {
		given: 'the expectation to not sync the data'
		latch = new CountDownLatch(1)

		// fail every time until limit is hit
		mockDataService.syncData(_ as ImportantData) >> {
			mockExecutions++
			fail()
		}

		when: 'a message is published'
		kafkaTemplate.send('retry-topic', "123", data)
		kafkaTemplate.flush()

		then: 'the latch will not change within 10 seconds'
		!latch.await(5, TimeUnit.SECONDS)
		mockExecutions == 5
	}
}
