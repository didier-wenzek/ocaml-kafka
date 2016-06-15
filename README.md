OCaml bindings for Kafka
====================================

Pre-requisites
--------------
* [OCaml](http://caml.inria.fr/)
* [Apache Kafka](http://kafka.apache.org/)
* [librdkafka](https://github.com/edenhill/librdkafka)

License
-------
MIT License

Install
-------

```sh
$ opam install kafka
```

From source:

```sh
$ make
$ make test       # assuming kakfa is running at localhost:9092 with a 'test' topic.
$ make install    # use ocamlfind
```

To enable Lwt support:

```sh
$ make lwt
$ make test
$ make install
```

Usage
-----

```ocaml
#use "topfind";;
#require "okafka";;

(* Prepare a producer handler. *)
let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"];;
let producer_topic = Kafka.new_topic producer "test" ["message.timeout.ms","10000"];;

(* Prepare a consumer handler *)
let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"];;
let consumer_topic = Kafka.new_topic consumer "test" ["auto.commit.enable","false"];;
let partition = 1;;
let timeout_ms = 1000;;

(* Start collecting messages *)
(* Here we start from offset_end, i.e. we will consume only messages produced from now. *)
Kafka.consume_start consumer_topic partition Kafka.offset_end;;

(* Produce some messages *)
Kafka.produce producer_topic partition "message 0";;
Kafka.produce producer_topic partition "message 1";;
Kafka.produce producer_topic partition "message 2";;

(* Consume messages *)
let rec consume t p = match Kafka.consume ~timeout_ms t p with
  | Kafka.Message(_,_,_,msg,_) -> msg
  | Kafka.PartitionEnd(_,_,_) -> consume t p
  | exception Kafka.Error(Kafka.TIMED_OUT,_) ->
    (Printf.fprintf stderr "Timeout after: %d ms\n%!" timeout_ms; consume t p)
in
let msg = consume consumer_topic partition in assert (msg = "message 0");
let msg = consume consumer_topic partition in assert (msg = "message 1");
let msg = consume consumer_topic partition in assert (msg = "message 2");

(* Stop collecting messages. *)
Kafka.consume_stop consumer_topic partition;;

(* Topics, consumers and producers must be released. *)
Kafka.destroy_topic producer_topic;;
Kafka.destroy_handler producer;;
Kafka.destroy_topic consumer_topic;;
Kafka.destroy_handler consumer;;
```

Documentation
-------------

The API is documented in [kafka.mli](kafka.mli),
and the Lwt extension is documented in [kafka_lwt.mli](kafka_lwt.mli).

See [tail_kafka_topic.ml](tail_kafka_topic.ml) for an example consumer using queues, batches and lwt.
See [sendto_kafka_topic.ml](sendto_kafka_topic.ml) for an example producer.

Configuration options of producers, consumers and topics
are inherited from [librdkafka/CONFIGURATION](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
