package com.mschroeder.kafka.streams;

import com.mschroeder.kafka.avro.Notification;
import com.mschroeder.kafka.avro.PackageEvent;
import com.mschroeder.kafka.avro.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Stream Description:
 *
 * This stream sources data from the package events topic
 * topic and first filters out those that are DELIVERED events.
 * Once these events are filtered, the stream gets rekeyed by
 * user id so that it can then be joined to the users KTable
 * created by the UserTable class.
 */
@Slf4j
@Configuration
public class DeliveryNotificationStream {
	public static final String SOURCE_TOPIC = "package-events";
	public static final String TARGET_TOPIC = "delivery-notifications";
	public static final String DELIVERED = "DELIVERED";

	@Bean
	public KStream<Integer, Notification> deliveredPackage(
			KTable<Integer, User> usersKTable,
			Serde<PackageEvent> packageEventSerde,
			Serde<User> userSerde,
			Serde<Notification> notificationSerde,
			StreamsBuilder streamsBuilder
	) {

		KStream<Integer, Notification> stream = streamsBuilder
				/* create stream from package events */
				.stream(
						SOURCE_TOPIC,
						Consumed.with(Serdes.Integer(), packageEventSerde)
				)
				/* log out the events */
				.peek((key, event) ->
						log.info("Event received: user-id={}, package-key={}, event-id={}, type={}",
								event.getUserId(),
								key,
								event.getEventId(),
								event.getEventType())
				)
				/* filter out the DELIVERED events */
				.filter((key, event) -> event.getEventType().equals(DELIVERED))
				/* rekey by user id so join can occur */
				.selectKey((key, value) -> value.getUserId())
				/* join to users ktable */
				.join(
						usersKTable,
						(event, user) -> Notification
								.newBuilder()
								.setEmail(user.getEmail())
								.setUserId(user.getUserId())
								.setType("DELIVERY")
								.build(),
						Joined.with(Serdes.Integer(), packageEventSerde, userSerde)
				);

		// pipe data to delivery notifications topic
		stream
				.peek((key, value) ->
						log.info("Publishing notification: user-id={}, email={}", key, value.getEmail())
				)
				.to(
						TARGET_TOPIC,
						Produced.with(Serdes.Integer(), notificationSerde)
				);

		return stream;
	}
}
