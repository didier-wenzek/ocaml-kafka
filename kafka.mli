(* Handler to a cluster of kafka brokers. *)
type handler

type error =
  (* Internal errors to rdkafka *)
  | BAD_MSG                             (* Received message is incorrect *)
  | BAD_COMPRESSION                     (* Bad/unknown compression *)
  | DESTROY                             (* Broker is going away *)
  | FAIL                                (* Generic failure *)
  | TRANSPORT                           (* Broker transport error *)
  | CRIT_SYS_RESOURCE                   (* Critical system resource failure *)
  | RESOLVE                             (* Failed to resolve broker.  *)
  | MSG_TIMED_OUT                       (* Produced message timed out. *)
  | UNKNOWN_PARTITION                   (* Permanent: Partition does not exist in cluster. *)
  | FS                                  (* File or filesystem error *)
  | UNKNOWN_TOPIC                       (* Permanent: Topic does not exist  in cluster. *)
  | ALL_BROKERS_DOWN                    (* All broker connections  are down. *)
  | INVALID_ARG                         (* Invalid argument, or invalid configuration *)
  | TIMED_OUT                           (* Operation timed out *)
  | QUEUE_FULL                          (* Queue is full *)
  | ISR_INSUFF                          (* ISR count < required.acks *)

  (* Standard Kafka errors *)
  | UNKNOWN
  | OFFSET_OUT_OF_RANGE
  | INVALID_MSG
  | UNKNOWN_TOPIC_OR_PART
  | INVALID_MSG_SIZE
  | LEADER_NOT_AVAILABLE
  | NOT_LEADER_FOR_PARTITION
  | REQUEST_TIMED_OUT
  | BROKER_NOT_AVAILABLE
  | REPLICA_NOT_AVAILABLE
  | MSG_SIZE_TOO_LARGE
  | STALE_CTRL_EPOCH
  | OFFSET_METADATA_TOO_LARGE

  (* Configuration errors *)
  | CONF_UNKNOWN                        (* Unknown configuration name. *)
  | CONF_INVALID                        (* Invalid configuration value. *)

exception Error of error * string

(* Create a kafka handler aimed to consume messages.

 - A single option is required : "metadata.broker.list", which is a comma sepated list of "host:port".
 - For a list of options,
   see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
   and https://kafka.apache.org/documentation.html#configuration
*)
val new_consumer : (string*string) list -> handler

(* Create a kafka handler aimed to produce messages.

 - A single option is required : "metadata.broker.list", which is a comma sepated list of "host:port".
 - For a list of options,
   see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
   and https://kafka.apache.org/documentation.html#configuration
*)
val new_producer :
     ?delivery_callback:(string -> error option -> unit)
  -> (string*string) list
  -> handler

(* Destroy Kafka handle (either a consumer or a producer) *)
val destroy_handler : handler -> unit

(* Kafka handle name *)
val handler_name : handler -> string

(* A handler to a kafka topic. *)
type topic

(* Creates a new topic handler for the kafka topic with the given name.

 - For a list of options,
   see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

 - For a producer, a partition_callback may be provided
   to assign a partition after the key provided by [produce_key_msg].
 *)
val new_topic :
    ?partitioner_callback:(int -> string-> int)  (* [partitioner partition_count key] assigns a partition for a key in [0..partition_count-1] *)
  -> handler                                     (* consumer or producer *)
  -> string                                      (* topic name *)
  -> (string*string) list                        (* topic option *)
  -> topic

(* Destroy topic handle *)
val destroy_topic : topic -> unit

(* Kafka topic handle name *)
val topic_name : topic -> string

(* [produce topic partition message]
  produces and sends a single message to the broker.

  The [partition] may be
  - either a proper partition (0..N-1)
  - or [Kafka.partition_unassigned].
*)
val produce: topic -> int -> string -> unit
val partition_unassigned: int

(* [produce_key_msg topic partition key message]
  produces and sends a (key,message) pair to the broker.
*)
val produce_key_msg: topic -> int -> string -> string -> unit

(* Returns the current out queue length: messages waiting to be sent to, or acknowledged by, the broker. *)
val outq_len : handler -> int

(* Polls the provided kafka handle for events.

  Events will cause application provided callbacks to be called.

  The 'timeout_ms' argument specifies the minimum amount of time
  (in milliseconds) that the call will block waiting for events.
  For non-blocking calls, provide 0 as 'timeout_ms'.
  To wait indefinately for an event, provide -1.

  Events:
  - delivery report callbacks (if dr_cb is configured) [producer]
  - error callbacks (if error_cb is configured) [producer & consumer]
  - stats callbacks (if stats_cb is configured) [producer & consumer]

  Returns the number of events served.
*)
val poll_events: ?timeout_ms:int -> handler -> int

(** Wait that messages are delivered. *)
val wait_delivery: ?timeout_ms:int -> ?max_outq_len:int -> handler -> unit

(* [consume_start topic partition offset]
  starts consuming messages for topic [topic] and [partition] at [offset].

  Offset may either be a proper offset (0..N)
  or one of the the special offsets:
  [Kafka.offset_beginning], [Kafka.offset_end], [Kafka.offset_stored]
  of [Kafka.offset_tail n] (i.e. n messages before [Kafka.offset_end]).
  
  The system (librdkafka) will attempt to keep 'queued.min.messages' (consumer config property)
  messages in the local queue by repeatedly fetching batches of messages
  from the broker until the threshold is reached.

  Raise an Error of error * string on error.
*)
val consume_start : topic -> int -> int64 -> unit
val offset_beginning: int64
val offset_end: int64
val offset_stored: int64
val offset_tail: int -> int64

(* [consume_stop topic partition]
   stop consuming messages for topic [topic] and [partition],
   purging all messages currently in the local queue.
*)
val consume_stop : topic -> int -> unit

type message =
  | Message of topic * int * int64 * string * string option (* topic, partition, offset, payload, optional key *)
  | PartitionEnd of topic * int * int64                     (* topic, partition, offset *)

(* [consume topic partition timeout_ms]
   consumes a single message from topic [topic] and [partition],
   waiting at most [timeout_ms] milli-seconds for a message to be received.

   Consumer must have been previously started with [Kafka.consume_start].
*)
val consume : topic -> int -> int -> message

(* A message queue allows the application to re-route consumed messages
   from multiple topics and partitions into one single queue point. *)
type queue

(* Create a new message queue. *)
val new_queue : handler -> queue

(* Destroy a message queue. *)
val destroy_queue : queue -> unit

(* [consume_start_queue queue topic partition offset]
  starts consuming messages for topic [topic] and [partition] at [offset]
  and routes consumed messages to the given queue.

  For a topic, either [consume_start] or [consume_start_queue] must be called.

  [consume_stop] has to be called to stop consuming messages from the topic.
*)
val consume_start_queue : queue -> topic -> int -> int64 -> unit

(* [consume_queue queue timeout_ms]
   consumes a single message from topics and partitions
   attached to the queue using [Kafka.consume_start_queue].

   Waits at most [timeout_ms] milli-seconds for a message to be received.
*)
val consume_queue : queue -> int -> message

(* [store_offset topic partition offset]
   stores [offset] for given [topic] and [partition].

   The offset will be commited (written) to the offset store according to the topic properties:
   - "offset.store.method" : "file" or "broker"
   - "offset.store.path"
   - "auto.commit.enable" : must be set to "false"
*)
val store_offset : topic -> int -> int64 -> unit

module Metadata : sig
  (* Topic information *)
  type topic_metadata = {
    topic_name: string;
    topic_partitions: int list;
  }
end

(* Topic information of a given topic. *)
val topic_metadata: ?timeout_ms:int -> handler -> topic -> Metadata.topic_metadata

(* Information of all local topics. *)
val local_topics_metadata: ?timeout_ms:int -> handler -> Metadata.topic_metadata list

(* Information of all topics known by the brokers. *)
val all_topics_metadata: ?timeout_ms:int -> handler -> Metadata.topic_metadata list

