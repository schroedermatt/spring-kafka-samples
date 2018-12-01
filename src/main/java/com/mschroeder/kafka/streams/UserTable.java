package com.mschroeder.kafka.streams;

import com.mschroeder.kafka.avro.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Stream Description:
 *
 * This stream pulls data from the user updates stream and updates
 * the underlying users KTable with the latest user information.
 */
@Slf4j
@Configuration
public class UserTable {
	static final String SOURCE_TOPIC = "user-updates";
	static final String KTABLE_NAME = "users";

	@Bean
	public KTable<Integer, User> usersKTable(Serde<User> userSerde, StreamsBuilder streamsBuilder) {
		return streamsBuilder.table(
					SOURCE_TOPIC,
					/* ktable state (/tmp/kafka-streams/streams-app/0_0/rocksdb/users) */
					Materialized.<Integer, User, KeyValueStore<Bytes, byte[]>>
							as(KTABLE_NAME)
							.withKeySerde(Serdes.Integer())
							.withValueSerde(userSerde)
							.withCachingDisabled()
							.withLoggingDisabled()
				);
	}
}
