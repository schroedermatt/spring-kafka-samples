package com.mschroeder.kafka.streams

import com.mschroeder.kafka.avro.Notification
import com.mschroeder.kafka.avro.PackageEvent
import com.mschroeder.kafka.avro.User
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.streams.KeyValue
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.StreamsBuilderFactoryBean
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import scala.Int
import spock.lang.Specification

import static com.mschroeder.kafka.util.IntegrationTestUtil.produceKeyValuesSynchronously
import static com.mschroeder.kafka.util.IntegrationTestUtil.readKeyValues
import static com.mschroeder.kafka.util.IntegrationTestUtil.readValues

@DirtiesContext
@SpringBootTest
@ActiveProfiles('test')
//@EmbeddedKafka(topics = ['user-updates', 'package-events', 'delivery-notifications'])
class DeliveryNotificationEmbeddedSpec extends Specification {
    // alternative setup to @EmbeddedKafka use
    public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(
            1,
            true,
            'user-updates',
            'package-events',
            'delivery-notifications'
    )

    @Autowired
    private StreamsBuilderFactoryBean factoryBean

    @Autowired
    private Producer<Integer, User> userProducer

    @Autowired
    private Producer<Integer, PackageEvent> packageEventProducer

    @Autowired
    private ConsumerFactory consumerFactory

    private Consumer<Integer, Notification> notificationConsumer

    def setup() {
        notificationConsumer = consumerFactory.createConsumer()
    }

    def setupSpec() {
        kafkaEmbedded.before()
    }

    def cleanupSpec() {
        kafkaEmbedded.after()
    }

    def 'delivery notification is published to topic'() {
        given: 'a user update published and stored in the ktable'
        User user = User.newBuilder()
                .setUserId(123)
                .setEmail('john.doe@email.com')
                .setNotificationsEnabled(false)
                .setUserName('john')
                .build()
        publishUserUpdate(user)

        and: 'a DELIVERED package event ready to be published'
        PackageEvent event = PackageEvent
                .newBuilder()
                .setEventId(1)
                .setUserId(user.userId)
                .setPackageId(345)
                .setEventType('DELIVERED')
                .build()

        when: 'publishing the event'
        publishPackageEvent(event)

        then: 'a delivery notification is sent to the delivery-notifications topic'
        List<Notification> notifications = readValues(
                DeliveryNotificationStream.TARGET_TOPIC,
                notificationConsumer,
                1
        )

        // expected notification is returned
        notifications.first() == Notification
                .newBuilder()
                .setUserId(user.userId)
                .setEmail(user.email)
                .setType('DELIVERY')
                .build()
    }

    def 'delivery notification is not published to topic'() {
        given: 'a user update published and stored in the ktable'
        User user = User.newBuilder()
                .setUserId(234)
                .setEmail('john.doe@email.com')
                .setNotificationsEnabled(false)
                .setUserName('john')
                .build()
        publishUserUpdate(user)

        and: 'a UNLOADED package event ready to be published'
        PackageEvent event = PackageEvent
                .newBuilder()
                .setEventId(1)
                .setUserId(user.userId)
                .setPackageId(345)
                .setEventType('UNLOADED')
                .build()

        when: 'publishing the event'
        publishPackageEvent(event)

        then: 'a delivery notification is NOT sent to the delivery-notifications topic'
        List<KeyValue<Integer, Notification>> notifications = readKeyValues(
                DeliveryNotificationStream.TARGET_TOPIC,
                notificationConsumer,
                1
        )

        // assert none of the messages on the topic are this test's user id
        notifications.each {
            assert it.key != user.userId
        }
    }

    private void publishUserUpdate(User user) {
        KeyValue record = new KeyValue<Integer, User>(user.userId, user)
        produceKeyValuesSynchronously(UserTable.SOURCE_TOPIC, [record], userProducer)
    }

    private void publishPackageEvent(PackageEvent event) {
        KeyValue record = new KeyValue<Integer, PackageEvent>(event.packageId, event)
        produceKeyValuesSynchronously(DeliveryNotificationStream.SOURCE_TOPIC, [record], packageEventProducer)
    }
}
