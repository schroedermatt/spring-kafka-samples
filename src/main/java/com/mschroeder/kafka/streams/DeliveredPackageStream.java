package com.mschroeder.kafka.streams;

import com.mschroeder.kafka.avro.PackageEvent;
import com.mschroeder.kafka.avro.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
public class DeliveredPackageStream {
	private static final String DELIVERED = "DELIVERED";

	@Bean
	public KStream<String, PackageEvent> deliveredPackage(
			KTable<String, User> usersKTable,
			Serde<PackageEvent> packageEventSerde,
			Serde<User> userSerde,
			StreamsBuilder streamsBuilder
	) {

		KStream<String, PackageEvent> stream = streamsBuilder
				/* create stream from package events */
				.stream(
						"package-events",
						Consumed.with(Serdes.String(), packageEventSerde)
				)
				/* log out the events */
				.peek((key, event) ->
						log.info("Event received: userId={}, packageKey={}, eventId={}, type={}",
								event.getUserId(),
								key,
								event.getEventId(),
								event.getEventType())
				)
				/* filter out the DELIVERED events */
				.filter((key, event) -> event.getEventType().equals(DELIVERED))
				/* join package events to transactions by key (package key) */
				.join(
						usersKTable,
						(event, users) -> event,
						Joined.with(Serdes.String(), packageEventSerde, userSerde)
				);

		// pipe data to delivered-transactions topic
		stream
				.peek((key, value) ->
						log.info("Publishing delivery event: userId={}, packageId={}", key, value.getPackageId())
				)
				.to(
						"delivered-package-events",
						Produced.with(Serdes.String(), packageEventSerde)
				);

		return stream;
	}
}
