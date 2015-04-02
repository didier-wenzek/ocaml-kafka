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
