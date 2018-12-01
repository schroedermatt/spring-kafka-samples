# Spring Kafka Samples

This repository is a playground for various features of Spring Kafka and related testing frameworks.

### Examples:
* JSON && Avro Listeners
* KTable - `UserTable`
* Streams (and table join) - `DeliveryNotificationStream`
* Spring Retry (See `RetryListenerSpec`)
* Streams Testing (See `UserTableSpec` && `DeliveryNotificationStreamSpec`)
* Embedded Testing (See `UserTableEmbeddedSpec` && `DeliveryNotificationEmbeddedSpec`)
* `MockSchemaRegistryClient` usage (see all tests)

### Testing Kafka Presentation

There is a slide deck available that walks through the ins and outs of
testing kafka applications. It references and utilizes a lot of sample code
from this repository.

Deck: https://www.testingkafka.schroedermatt.com/