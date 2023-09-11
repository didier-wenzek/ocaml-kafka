(** Asynchronous Kafka interface using Lwt.

   For consumers, things work mostly as without Lwt
   and non blocking versions are only provided for consume functions
   ([Kafka_lwt.consume], [Kafka_lwt.consume_queue], [Kafka_lwt.consume_batch] and [Kafka_lwt.consume_batch_queue]).
   These functions use topics or queues prepared
   using either [Kafka.new_topic] or [Kafka.new_queue];
   and assume that consuption has been started using
   either [Kafka.consume_start] or [Kafka.consume_start_queue];

   For producers, the Lwt interface mainly adds support for delivery reporting;
   Since [Kafka.produce] is already non-blocking, but delivery has to be checked using
   callbacks and explicit calls to [Kafka.poll] (which is blocking).
   For that, the specific [Kafka_lwt.new_producer] and [Kafka_lwt.produce] functions
   has to be used, setting in the background a periodic non-blocking check of message delivery
   and awaking [Kafka_lwt.produce] when delivery succeeds or fails.
*)

val consume :
  ?timeout_ms:int -> Kafka.topic -> Kafka.partition -> Kafka.message Lwt.t
(** [consume ~timeout_ms topic partition]
   consumes a single message from topic [topic] and [partition].
   
   Waits at most [timeout_ms] milli-seconds for a message to be received.
   (The default timout is 1 second)

   Consumer must have been previously started with [Kafka.consume_start].
*)

val consume_queue : ?timeout_ms:int -> Kafka.queue -> Kafka.message Lwt.t
(** [consume_queue ~timeout_ms queue]
   consumes a single message from topics and partitions
   attached to the queue using [Kafka.consume_start_queue].

   Waits at most [timeout_ms] milli-seconds for a message to be received.
   The default timout is 1 second.
*)

val consume_batch :
  ?timeout_ms:int ->
  ?msg_count:int ->
  Kafka.topic ->
  Kafka.partition ->
  Kafka.message list Lwt.t
(** [consume_batch ~timeout_ms ~msg_count topic partition]
   consumes up to [msg_count] messages from [topic] and [partition],
   taking at most [timeout_ms] to collect the messages
   (hence, it may return less messages than requested).

   The default timout is 1 second.
   The default count of messages is 1k.
*)

val consume_batch_queue :
  ?timeout_ms:int -> ?msg_count:int -> Kafka.queue -> Kafka.message list Lwt.t
(** [consume_batch_queue ~timeout_ms ~msg_count queue]
   consumes up to [msg_count] messages from the [queue],
   taking at most [timeout_ms] to collect the messages
   (hence, it may return less messages than requested).

   The default timout is 1 second.
   The default count of messages is 1k.
*)

val new_producer :
  ?delivery_check_period_ms:int -> (string * string) list -> Kafka.handler
(** Create a kafka handler aimed to produce messages using [Kafka_lwt.produce].

 - Same options as [Kafka.new_producer],
   but the option 'delivery.report.only.error' which is enforced to false.

 - No delivery callback:
   such a callback is implicitly set to awake on message delivery
   the [Lwt.t] thread returned by [Kafka_lwt.produce].
*)

val produce :
  Kafka.topic ->
  ?partition:Kafka.partition ->
  ?key:string ->
  string ->
  unit Lwt.t
(** Produces and sends a single message to the broker.

  Same parameters as [Kafka.produce] but without message id (the latter beeing assigned by the system).

  Immediately returns a [Lwt.t] thread, which will be awaken on success or failure.
*)

val wait_delivery :
  ?timeout_ms:int -> ?max_outq_len:int -> Kafka.handler -> unit Lwt.t
(** Wait that messages are delivered (waiting that less than max_outq_len messages are pending). *)
