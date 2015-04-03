(* Asynchronous Kafka interface using Lwt. *)

(* [consume ~timeout_ms topic partition]
   consumes a single message from topic [topic] and [partition].
   
   Waits at most [timeout_ms] milli-seconds for a message to be received.
   (The default timout is 1 second)

   Consumer must have been previously started with [Kafka.consume_start].
*)
val consume : ?timeout_ms:int -> Kafka.topic -> Kafka.partition -> Kafka.message Lwt.t

(* [consume_queue ~timeout_ms queue]
   consumes a single message from topics and partitions
   attached to the queue using [Kafka.consume_start_queue].

   Waits at most [timeout_ms] milli-seconds for a message to be received.
   The default timout is 1 second.
*)
val consume_queue : ?timeout_ms:int -> Kafka.queue -> Kafka.message Lwt.t

(* [consume_batch ~timeout_ms ~msg_count topic partition]
   consumes up to [msg_count] messages from [topic] and [partition],
   taking at most [timeout_ms] to collect the messages
   (hence, it may return less messages than requested).

   The default timout is 1 second.
   The default count of messages is 1k.
*)
val consume_batch : ?timeout_ms:int -> ?msg_count:int -> Kafka.topic -> Kafka.partition -> Kafka.message list Lwt.t

(* [consume_batch_queue ~timeout_ms ~msg_count queue]
   consumes up to [msg_count] messages from the [queue],
   taking at most [timeout_ms] to collect the messages
   (hence, it may return less messages than requested).

   The default timout is 1 second.
   The default count of messages is 1k.
*)
val consume_batch_queue : ?timeout_ms:int -> ?msg_count:int -> Kafka.queue -> Kafka.message list Lwt.t
