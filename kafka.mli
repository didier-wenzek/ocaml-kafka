(* Handler to a cluster of kafka brokers. *)
type handler

(* Create a kafka handler aimed to consume messages.

 - A single option is required : "metadata.broker.list", which is a comma sepated list of "host:port".
 - For a list of options,
   see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
   and https://kafka.apache.org/documentation.html#configuration
*)
val new_consumer : (string*string) list -> handler

(* Create a kafka handler aimed to consume messages.

 - A single option is required : "metadata.broker.list", which is a comma sepated list of "host:port".
 - For a list of options,
   see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
   and https://kafka.apache.org/documentation.html#configuration
*)
val new_producer : (string*string) list -> handler

val destroy_handler : handler -> unit
val handler_name : handler -> string

(* A named topic to which message can be produced or consumed. *)
type topic
val new_topic : handler -> string -> (string*string) list -> topic
val destroy_topic : topic -> unit
val topic_name : topic -> string
val topic_partition_available: topic -> int -> bool

(* [produce topic partition message] *)
val produce: topic -> int -> string -> unit
val partition_unassigned: int

(* [consume_start topic partition offset]
  starts consuming messages for topic [topic] and [partition] at [offset].

  Offset may either be a proper offset (0..N)
  or one of the the special offsets:
  [Kafka.offset_beginning], [Kafka.offset_end].
  
  rdkafka will attempt to keep 'queued.min.messages' (consumer config property)
  messages in the local queue by repeatedly fetching batches of messages
  from the broker until the threshold is reached.

  Raise an Error of error * string on error.
*)
val consume_start : topic -> int -> int64 -> unit
val offset_beginning: int64
val offset_end: int64

(* [consume_stop topic partition]
   stop consuming messages for topic [topic] and [partition],
   purging all messages currently in the local queue.
*)
val consume_stop : topic -> int -> unit

type message = {
    topic: string;
    partition: int;
    offset: int64;
    payload: string;
}

(* [consume topic partition timeout_ms]
   consumes a single message from topic [topic] and [partition],
   waiting at most [timeout_ms] milli-seconds for a message to be received.

   Consumer must have been previously started with [Kafka.consume_start].
*)
val consume : topic -> int -> int -> int64*string

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
  | PARTITION_EOF                       (* Reached the end of the topic+partition queue on the broker.  Not really an error. *)
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
  | NO_ERROR
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
