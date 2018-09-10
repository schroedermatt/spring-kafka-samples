package com.mschroeder.kafka.listener

import com.mschroeder.kafka.config.MockBeanFactory
import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka(topics = ['retry-topic'])
@Import([MockBeanFactory])
class RetryListenerSpec extends Specification {
	private CountDownLatch latch

	@Autowired
	KafkaEmbedded kafkaEmbedded

	@Autowired
	KafkaTemplate<String, ImportantData> kafkaTemplate

	@Autowired
	ImportantDataServiceImpl mockImportantDataService

	def cleanup() {
		kafkaEmbedded.after()
	}

	def 'retry stuff'() {
		given:
		latch = new CountDownLatch(1)

		def data = new ImportantData()
		data.id = 1
		data.name = 'testing'
		data.description = 'this is just a retry, this is just a retry'

		when:
		kafkaTemplate.send('retry-topic', "test1", data)
		kafkaTemplate.flush()

		then:
//		2 * mockImportantDataService.syncData(_) >> { throw new KafkaException("ugh") }
		mockImportantDataService.syncData(_) >> { latch.countDown() }

		latch.await(20, TimeUnit.SECONDS)
	}
}
