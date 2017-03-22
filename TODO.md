Basic:
 - [x] Consumer,Producer,Topic CRUD.
 - [x] Produce and consume messages.
 - [x] Support for topic properties.
 - [x] Support to consume batch of messages.
 - [ ] Support to produce batch of messages.
 - [x] Support for queues.
 - [x] Support for keys.
 - [x] Lwt support.
 - [ ] Support for the high-level KafkaConsumer interface (rd_kafka_subscribe)

Callbacks:
 - [x] Support for delivery report callback.
 - [ ] Support for error and log callbacks.
 - [ ] Support for statistics callback.
 - [x] Support for msg_opaque data on produce/delivery_callback.
 - [ ] Fix: the partitionner callback is never called.
 - [ ] Support to consume messages through a callback.

Messages
 - [x] Wrap messages in message enveloppes with topic,partition,offset,payload,key.
 - [x] Improve error handling on consume : PartitionEnd vs timeout or error.
 - [ ] Support for timestamp of a message.

Offsets
 - [x] Support for stored offsets.
 - [x] Specific offsets: Kafka.offset_stored, Kafka.offset_tail(n).
 - [ ] Support for look up of offsets by timestamp.
 - [ ] rd_kafka_commit_queue

Resources
 - [x] Free messages once consumed using rd_kafka_message_destroy.
 - [ ] Function: Kafka.wait_destroyed.
 - [ ] rd_kafka_flush

Meta-data
 - [ ] Function: Kafka.version: unit -> string.
 - [x] Meta-data: topic list, partition list.
 - [ ] Meta-data: server list, list of replica.

Misc
 - [x] Function: Kafka.outq_len.
 - [ ] rd_kafka_queue_length
 - [ ] Do we have to distinguish types: consumer vs producer ?
 - [ ] Introduce a Kafka.Timeout exception.
 - [ ] Support for events.
 
