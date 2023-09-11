type handler
type topic
type queue
type partition = int
type offset = int64

type message =
  | Message of
      topic
      * partition
      * offset
      * string
      * string option (* topic, partition, offset, payload, optional key *)
  | PartitionEnd of topic * partition * offset (* topic, partition, offset *)

type msg_id = int

type error =
  (* Internal errors to rdkafka: *)
  | BAD_MSG (* Received message is incorrect *)
  | BAD_COMPRESSION (* Bad/unknown compression *)
  | DESTROY (* Broker is going away *)
  | FAIL (* Generic failure *)
  | TRANSPORT (* Broker transport error *)
  | CRIT_SYS_RESOURCE (* Critical system resource failure *)
  | RESOLVE (* Failed to resolve broker.  *)
  | MSG_TIMED_OUT (* Produced message timed out. *)
  | UNKNOWN_PARTITION (* Permanent: Partition does not exist in cluster. *)
  | FS (* File or filesystem error *)
  | UNKNOWN_TOPIC (* Permanent: Topic does not exist  in cluster. *)
  | ALL_BROKERS_DOWN (* All broker connections  are down. *)
  | INVALID_ARG (* Invalid argument, or invalid configuration *)
  | TIMED_OUT (* Operation timed out *)
  | QUEUE_FULL (* Queue is full *)
  | ISR_INSUFF (* ISR count < required.acks *)
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
  | CONF_UNKNOWN (* Unknown configuration name. *)
  | CONF_INVALID (* Invalid configuration value. *)

exception Error of error * string

let _ =
  Callback.register_exception "kafka.error" (Error (UNKNOWN, "msg string"))

external new_consumer : (string * string) list -> handler
  = "ocaml_kafka_new_consumer"

external new_producer :
  ?delivery_callback:(msg_id -> error option -> unit) ->
  (string * string) list ->
  handler = "ocaml_kafka_new_producer"

external destroy_handler : handler -> unit = "ocaml_kafka_destroy_handler"
external handler_name : handler -> string = "ocaml_kafka_handler_name"

external new_topic :
  ?partitioner_callback:(int -> string -> partition option) ->
  handler ->
  string ->
  (string * string) list ->
  topic = "ocaml_kafka_new_topic"

external destroy_topic : topic -> unit = "ocaml_kafka_destroy_topic"
external topic_name : topic -> string = "ocaml_kafka_topic_name"

(*
  Note that the id is restricted to be some int value.
  While the underlying library, librdkafka, allows any void* msg_opaque data.
  This is to avoid issues with the garbage collector
*)
external produce_idmsg :
  topic -> ?partition:int -> ?key:string -> msg_id -> string -> unit
  = "ocaml_kafka_produce"

let produce topic ?partition ?key ?(msg_id = 0) msg =
  produce_idmsg topic ?partition ?key msg_id msg

external flush : handler -> int -> unit = "ocaml_kafka_flush"

let flush ?(timeout_ms = 1000) handler = flush handler timeout_ms

external outq_len : handler -> int = "ocaml_kafka_outq_len"
external poll : handler -> int -> int = "ocaml_kafka_poll"

let poll_events ?(timeout_ms = 1000) handler = poll handler timeout_ms

let wait_delivery ?(timeout_ms = 100) ?(max_outq_len = 0) handler =
  let rec loop () =
    if outq_len handler > max_outq_len then (
      ignore (poll_events ~timeout_ms handler);
      loop ())
    else ()
  in
  loop ()

external consume_start : topic -> partition -> offset -> unit
  = "ocaml_kafka_consume_start"

external consume_stop : topic -> partition -> unit = "ocaml_kafka_consume_stop"

let offset_beginning = -2L
let offset_end = -1L
let offset_stored = -1000L
let offset_tail n = Int64.sub (-2000L) (Int64.of_int n)

external consume : ?timeout_ms:int -> topic -> partition -> message
  = "ocaml_kafka_consume"

external consume_batch :
  ?timeout_ms:int -> ?msg_count:int -> topic -> partition -> message list
  = "ocaml_kafka_consume_batch"

external store_offset : topic -> partition -> offset -> unit
  = "ocaml_kafka_store_offset"

external new_queue : handler -> queue = "ocaml_kafka_new_queue"
external destroy_queue : queue -> unit = "ocaml_kafka_destroy_queue"

external consume_start_queue : queue -> topic -> partition -> offset -> unit
  = "ocaml_kafka_consume_start_queue"

external consume_queue : ?timeout_ms:int -> queue -> message
  = "ocaml_kafka_consume_queue"

external consume_batch_queue :
  ?timeout_ms:int -> ?msg_count:int -> queue -> message list
  = "ocaml_kafka_consume_batch_queue"

module Metadata = struct
  type topic_metadata = {
    topic_name : string;
    topic_partitions : partition list;
  }
end

external get_topic_metadata : handler -> topic -> int -> Metadata.topic_metadata
  = "ocaml_kafka_get_topic_metadata"

external get_topics_metadata :
  handler -> bool -> int -> Metadata.topic_metadata list
  = "ocaml_kafka_get_topics_metadata"

let topic_metadata ?(timeout_ms = 1000) handler topic =
  get_topic_metadata handler topic timeout_ms

let local_topics_metadata ?(timeout_ms = 1000) handler =
  get_topics_metadata handler false timeout_ms

let all_topics_metadata ?(timeout_ms = 1000) handler =
  get_topics_metadata handler true timeout_ms

external get_librdkafka_version : unit -> string
  = "ocaml_kafka_get_librdkafka_version"

let librdkafka_version = get_librdkafka_version ()

external offset_store : topic -> partition -> offset -> unit
  = "ocaml_kafka_offset_store"
