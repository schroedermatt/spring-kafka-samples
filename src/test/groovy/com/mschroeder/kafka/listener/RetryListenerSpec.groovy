package com.mschroeder.kafka.listener

import com.mschroeder.kafka.config.BaseKafkaSpecification
import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataService
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Slf4j
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

	@Value('${topics.retry-data}')
	private String topic

	@Autowired
	KafkaTemplate<String, ImportantData> kafkaTemplate

	@Autowired
	ImportantDataService mockDataService

	def setup() {
		latch = new CountDownLatch(1)
		mockExecutions = 0
	}

	def cleanup() {
		mockExecutions = 0
	}

	def 'buffer for 2 seconds to let embedded kafka come up'() {
		expect:
		Thread.sleep(2000)
		true
	}

	def 'listener will retry 4 times and succeed'() {
		given: 'the expectation to sync the data once'
		// fail 3 times and then succeed
		mockDataService.syncData(_ as ImportantData) >> {
			mockExecutions++

			if (mockExecutions < 4) {
				fail()
			}

			succeed()
		}

		when: 'a message is published'
		sendMessage()

		then: 'the latch will countdown once within 2 seconds'
		latch.await(2, TimeUnit.SECONDS)
		mockExecutions == 4
	}

	def 'listener will fail after retrying the limit of 5 attempts'() {
		given: 'the expectation to not sync the data'
		// fail every time until limit is hit
		mockDataService.syncData(_ as ImportantData) >> {
			mockExecutions++
			fail()
		}

		when: 'a message is published'
		sendMessage()

		then: 'the latch will not change within 2 seconds'
		!latch.await(2, TimeUnit.SECONDS)
		mockExecutions == 5
	}

	def 'listener will fail if non KafkaException is thrown'() {
		given: 'the expectation to not sync the data'
		// fail with a RuntimeException
		mockDataService.syncData(_ as ImportantData) >> {
			mockExecutions++
			log.error("UNEXPECTED RUNTIME FAILURE ON EXECUTION #{}", mockExecutions)
			throw new RuntimeException("oops!")
		}

		when: 'a message is published'
		sendMessage()

		then: 'the latch will not change within 2 seconds'
		!latch.await(2, TimeUnit.SECONDS)
		mockExecutions == 5
	}

	/** PRIVATE TEST HELPERS **/

	private void fail() {
		log.error("FAILING ON EXECUTION #{}", mockExecutions)
		throw new KafkaException("oops!")
	}

	private void succeed() {
		log.info("SUCCEEDING ON EXECUTION #{}", mockExecutions)
		latch.countDown()
	}

	private void sendMessage() {
		kafkaTemplate.send(topic, "123", data)
		kafkaTemplate.flush()
	}
}
