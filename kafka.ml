type handler

external new_consumer : (string*string) list -> handler = "ocaml_kafka_new_consumer"
external new_producer : (string*string) list -> handler = "ocaml_kafka_new_producer"

external destroy_handler : handler -> unit = "ocaml_kafka_destroy_handler"
external handler_name : handler -> string = "ocaml_kafka_handler_name"

type topic
external new_topic : handler -> string -> (string*string) list -> topic = "ocaml_kafka_new_topic"
external destroy_topic : topic -> unit = "ocaml_kafka_destroy_topic"
external topic_name : topic -> string = "ocaml_kafka_topic_name"
external topic_partition_available: topic -> int -> bool = "ocaml_kafka_topic_partition_available"

external produce: topic -> int -> string -> unit = "ocaml_kafka_produce"
external consume_start : topic -> int -> int64 -> unit = "ocaml_kafka_consume_start"
external consume_stop : topic -> int -> unit = "ocaml_kafka_consume_stop"

let partition_unassigned = -1
let offset_beginning = -2L
let offset_end = -1L
let offset_stored = -1000L
let offset_tail n = Int64.sub (-2000L) (Int64.of_int n)

type message =
  | Message of topic * int * int64 * string              (* topic, partition, offset, payload *)
  | PartitionEnd of topic * int * int64                  (* topic, partition, offset *)

external consume : topic -> int -> int -> message = "ocaml_kafka_consume"
external store_offset : topic -> int -> int64 -> unit = "ocaml_kafka_store_offset"

type queue
external new_queue : handler -> queue = "ocaml_kafka_new_queue"
external destroy_queue : queue -> unit = "ocaml_kafka_destroy_queue"
external consume_start_queue : queue -> topic -> int -> int64 -> unit = "ocaml_kafka_consume_start_queue"
external consume_queue : queue -> int -> message = "ocaml_kafka_consume_queue"

type error =
  (* Internal errors to rdkafka: *)
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

  (* Standard Kafka errors: *)
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

let _ = 
  Callback.register_exception "kafka.error" (Error(UNKNOWN,"msg string"));

