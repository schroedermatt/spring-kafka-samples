package com.mschroeder.kafka

import com.mschroeder.kafka.config.BaseKafkaSpecification

class ListenerApplicationSpec extends BaseKafkaSpecification {
	def 'context loads'() {
		expect:
		true
	}
}
