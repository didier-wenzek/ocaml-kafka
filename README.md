OCaml bindings for Kafka
====================================

[![Build Status](https://travis-ci.org/didier-wenzek/ocaml-kafka.svg?branch=master)](https://travis-ci.org/didier-wenzek/ocaml-kafka)

Pre-requisites
--------------
* [OCaml](http://caml.inria.fr/) (`ocaml-version >= "4.02.3"`)
* [Apache Kafka](http://kafka.apache.org/)
* [librdkafka](https://github.com/edenhill/librdkafka) (`version >= 0.8.6`)

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
$ make            # use dune
$ make test       # assuming kafka is running at localhost:9092 with a 'test' topic.
$ make install    # use opam
```

Usage
-----

```ocaml
#use "topfind";;
#require "kafka";;

(* Prepare a producer handler. *)
let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"];;
let producer_topic = Kafka.new_topic producer "test" ["message.timeout.ms","10000"];;

(* Prepare a consumer handler *)
let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"];;
let consumer_topic = Kafka.new_topic consumer "test" ["auto.commit.enable","false"];;
let partition = 0;;
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

The API is documented in [lib/kafka.mli](lib/kafka.mli),
and the Lwt extension is documented in [lib_lwt/kafka_lwt.mli](lib_lwt/kafka_lwt.mli).

See [bin/tail_kafka_topic.ml](bin/tail_kafka_topic.ml) for an example consumer using queues, batches and lwt.
See [bin/sendto_kafka_topic.ml](bin/sendto_kafka_topic.ml) for an example producer.

Configuration options of producers, consumers and topics
are inherited from [librdkafka/CONFIGURATION](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
