(lang dune 1.10)
(name kafka)
(generate_opam_files true)
(maintainers "Didier Wenzek <didier.wenzek@acidalie.com>")
(authors "Didier Wenzek <didier.wenzek@acidalie.com>")
(bug_reports "https://github.com/didier-wenzek/ocaml-kafka/issues")
(homepage "https://github.com/didier-wenzek/ocaml-kafka")
(license "MIT")
(source (github "didier-wenzek/ocaml-kafka"))

(package
  (name kafka)
  (depends
    (ocaml (>= 4.02.3))
    conf-zlib
    (alcotest (and (>= 0.8.5) :with-test))
    (dune (>= 1.10)))
  (synopsis "OCaml bindings for Kafka")
  (description "
Kafka is a high-throughput distributed messaging system.
"))

(package
  (name kafka_lwt)
  (depends
    (ocaml (>= 4.02.3))
    (kafka (= :version))
    cmdliner
    lwt
    conf-zlib
    (alcotest-lwt (and (>= 0.8.5) :with-test))
    (dune (>= 1.10)))
  (synopsis "OCaml bindings for Kafka, Lwt bindings")
  (description "
Kafka is a high-throughput distributed messaging system.
"))

(package
  (name kafka_async)
  (depends
    (ocaml (>= 4.03.0))
    (kafka (= :version))
    (async (>= v0.11))
    (core (>= v0.11))
    (alcotest-async (and (>= 0.8.5) :with-test))
    (dune (>= 1.10)))
  (synopsis "OCaml bindings for Kafka, Async bindings")
  (description "
Kafka is a high-throughput distributed messaging system.
"))
