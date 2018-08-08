package com.mschroeder.kafka

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import spock.lang.Specification

@SpringBootTest
@EmbeddedKafka
class ListenerApplicationSpec extends Specification {
	def 'context loads'() {
		expect:
		true
	}
}
