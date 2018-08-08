# Spring Kafka Listener



### Local Development

#### Reset Consumer Group

1. exec onto broker container
2. look up consumer groups and reset the offset

_list consumer groups_
```bash
> kafka-consumer-groups --bootstrap-server broker:9092 --list
```

_show details of consumer group_
```bash
> kafka-consumer-groups --bootstrap-server broker:9092 --group <group_id> --describe
```

_reset offset to earliest_
```
> kafka-consumer-groups --bootstrap-server broker:9092 --group <group_id> 
    --topic <topic_name> --reset-offsets --to-earliest --execute
```