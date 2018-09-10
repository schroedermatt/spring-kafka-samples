package com.mschroeder.kafka.listener

import com.mschroeder.kafka.domain.ImportantData
import com.mschroeder.kafka.service.ImportantDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import spock.lang.Specification

import java.util.concurrent.TimeUnit;

@SpringBootTest
@EmbeddedKafka
class RetryListenerSpec extends Specification {
	@Autowired
	KafkaEmbedded kafkaEmbedded

	@Autowired
	KafkaTemplate<String, ImportantData> kafkaTemplate

	@Autowired
	CountdownLatchRetryListener retryListener

	@Autowired
	ImportantDataService mockImportantDataService

	def cleanup() {
		kafkaEmbedded.after()
	}

	def 'retry stuff'() {
		given:
		def data = new ImportantData()
		data.id = 1
		data.name = 'testing'
		data.description = 'this is just a retry, this is just a retry'

		when:
		kafkaTemplate.send('key1', data)
		kafkaTemplate.flush()

		then:
		1 * mockImportantDataService.syncData(_) >> { throw new KafkaException("ugh") }
		1 * mockImportantDataService.syncData(_) >> { /** noop **/ }

		retryListener.latch.await(20, TimeUnit.SECONDS)
	}
}
