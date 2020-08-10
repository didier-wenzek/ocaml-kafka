0.5
===

* Add Async bindings
* Split sync and Lwt variants into separate OPAM packages
* Fix compatibility with OCaml 4.09
* Register callbacks as GC roots
* Port to `dune`
* Port to OPAM 2 format
* Make `dune` generate `opam` files automatically
* Add Changelog to facilitate releases with `dune-release`
* Depend ob `zlib` depext instead of explicit package names

0.4
===

* Fix build instructions

0.3.2
=====

* Fix the opam file in order to use opam publish

0.3.1
=====

* Fix the package name to match the name actually used by OPAM

0.3
===

* Replace use of `int32` by `int32_t` to be compatible with OCaml 4.03.0
* Fix `Kafka_lwt.consume_batch` is not thread-safe
* Fix `rd_kafka_conf` leak
* Port to `jbuilder`

0.2
===

* Support for asynchronous consumers using Lwt

0.1
===

* Support to write Kafka consumers and producers
