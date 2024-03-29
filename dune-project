(lang dune 2.0)
(name kafka)
(generate_opam_files true)
(maintainers "Didier Wenzek <didier.wenzek@acidalie.com>")
(authors "Didier Wenzek <didier.wenzek@acidalie.com>")
(bug_reports "https://github.com/didier-wenzek/ocaml-kafka/issues")
(homepage "https://github.com/didier-wenzek/ocaml-kafka")
(documentation "https://didier-wenzek.github.io/ocaml-kafka")
(license "MIT")
(source (github "didier-wenzek/ocaml-kafka"))

(package
  (name kafka)
  (depends
    (ocaml (>= 4.02.3))
    conf-zlib
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
    (cmdliner (>= 1.1.0))
    lwt
    conf-zlib
    (dune (>= 1.10)))
  (synopsis "OCaml bindings for Kafka, Lwt bindings")
  (description "
Kafka is a high-throughput distributed messaging system.
"))

(package
  (name kafka_async)
  (depends
    (ocaml (>= 4.03.0))
    (ppx_let(>= v0.16))
    (kafka (= :version))
    (async (>= v0.16))
    (core (>= v0.16))
    (core_unix (>= v0.16))
    (dune (>= 1.10)))
  (synopsis "OCaml bindings for Kafka, Async bindings")
  (description "
Kafka is a high-throughput distributed messaging system.
"))
