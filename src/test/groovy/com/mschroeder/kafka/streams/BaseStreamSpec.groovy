package com.mschroeder.kafka.streams

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Shared
import spock.lang.Specification

abstract class BaseStreamSpec extends Specification {
    protected static final String APP_ID = "topology-tests"
    protected static final String BROKER_ID = "mock-broker:1234"

    protected TopologyTestDriver testDriver

    @Shared
    protected Properties defaultProps
    @Shared
    protected MockSchemaRegistryClient mockSchemaRegistry

    // run once before all tests
    def setupSpec() {
        mockSchemaRegistry = new MockSchemaRegistryClient()
//        testDataFactory = new TestDataFactory()

        // setup default stream props
        defaultProps = new Properties()
        defaultProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID)
        defaultProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_ID)
        defaultProps.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName())
    }

    // run before every test
    def setup() {
        configureTestDriver()

        if (testDriver == null) {
            throw new UnsupportedOperationException("TopologyTestDriver was not configured")
        }
    }

    // run after every test
    def cleanup() {
        if (testDriver != null) {
            testDriver.close()
        }
    }

    abstract void configureTestDriver()
}
