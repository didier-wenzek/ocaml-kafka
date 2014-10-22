type handler

external new_consumer : (string*string) list -> handler = "ocaml_kafka_new_consumer"
external new_producer : (string*string) list -> handler = "ocaml_kafka_new_producer"

external destroy_handler : handler -> unit = "ocaml_kafka_destroy_handler"
external handler_name : handler -> string = "ocaml_kafka_handler_name"

type topic
external new_topic : handler -> string -> topic = "ocaml_kafka_new_topic"
external destroy_topic : topic -> unit = "ocaml_kafka_destroy_topic"
external topic_name : topic -> string = "ocaml_kafka_topic_name"
external topic_partition_available: topic -> int -> bool = "ocaml_kafka_topic_partition_available"

external produce: topic -> int -> string -> unit = "ocaml_kafka_produce"
external consume_start : topic -> int -> int64 -> unit = "ocaml_kafka_consume_start"
external consume_stop : topic -> int -> unit = "ocaml_kafka_consume_stop"

type message = {
    topic: string;
    partition: int;
    offset: int64;
    payload: string;
}

external consume : topic -> int -> int -> int64*string = "ocaml_kafka_consume"

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
  | PARTITION_EOF                       (* Reached the end of the topic+partition queue on the broker.  Not really an error. *)
  | UNKNOWN_PARTITION                   (* Permanent: Partition does not exist in cluster. *)
  | FS                                  (* File or filesystem error *)
  | UNKNOWN_TOPIC                       (* Permanent: * Topic does not exist  in cluster. *)
  | ALL_BROKERS_DOWN                    (* All broker connections  are down. *)
  | INVALID_ARG                         (* Invalid argument, or invalid configuration *)
  | TIMED_OUT                           (* Operation timed out *)
  | QUEUE_FULL                          (* Queue is full *)
  | ISR_INSUFF                          (* ISR count < required.acks *)

  (* Standard Kafka errors: *)
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

let _ = 
  Callback.register_exception "kakfa.error" (Error(UNKNOWN,"msg string"));

