open Async

type producer

type consumer

type 'a response = ('a, Kafka.error * string) result

val produce :
  producer ->
  Kafka.topic ->
  ?partition:Kafka.partition ->
  ?key:string ->
  string ->
  unit response Deferred.t

val new_producer : (string * string) list -> producer response

val new_consumer : (string * string) list -> consumer response

val new_topic :
  producer -> string -> (string * string) list -> Kafka.topic response

val consume : consumer -> topic:string -> Kafka.message Pipe.Reader.t response

val destroy_consumer : consumer -> unit

val destroy_producer : producer -> unit
