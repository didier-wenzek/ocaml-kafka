(* [consume ~timeout_ms topic partition]
   consumes a single message from topic [topic] and [partition].
   
   Waits at most [timeout_ms] milli-seconds for a message to be received.
   (The default timout is 1 second)

   Consumer must have been previously started with [Kafka.consume_start].
*)
val consume : ?timeout_ms:int -> Kafka.topic -> Kafka.partition -> Kafka.message Lwt.t
