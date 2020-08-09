open Core
open Async

let main_result host port topic =
  let open Deferred.Result.Let_syntax in
  let rng = Random.State.make_self_init () in
  let make_random_message rng =
    let bit = Random.State.int rng Int.max_value in
    Printf.sprintf "message %d" bit
  in
  let producer_options =
    [ ("metadata.broker.list", Printf.sprintf "%s:%d" host port) ]
  in
  let%bind producer =
    Deferred.return @@ Kafka_async.new_producer producer_options
  in
  Log.Global.debug "Created producer";
  let%bind producer_topic =
    Deferred.return
    @@ Kafka_async.new_topic producer topic [ ("message.timeout.ms", "1000") ]
  in
  Log.Global.debug "Created topic";
  let%bind consumer =
    Deferred.return
    @@ Kafka_async.new_consumer
         [
           ("metadata.broker.list", Printf.sprintf "%s:%d" host port);
           ("group.id", "test-consumer-groups");
           ("auto.offset.reset", "earliest");
         ]
  in
  Log.Global.debug "Created consumer";
  let partition = 0 in
  let messages = List.init 3 ~f:(Fn.const (make_random_message rng)) in
  let%bind _ =
    messages
    |> List.map ~f:(Kafka_async.produce producer producer_topic partition)
    |> Deferred.Result.all
  in
  Log.Global.debug "Emitted messages";
  let%bind reader = Deferred.return @@ Kafka_async.consume consumer ~topic in
  let%bind _dummy_reader = Deferred.return @@ Kafka_async.consume consumer ~topic:"dummy_topic_pull_request_23" in
  let%bind consumed =
    Deferred.ok
    @@ Pipe.fold reader ~init:(String.Set.of_list messages) ~f:(fun awaiting ->
           function
           | Message (topic, _, _, payload, _) ->
               let topic_name = Kafka.topic_name topic in
               Log.Global.debug "Message on topic '%s', payload '%s'" topic_name
                 payload;
               let remaining = String.Set.remove awaiting payload in
               (match String.Set.is_empty remaining with
               | true -> Pipe.close_read reader
               | false -> ());
               Deferred.return remaining
           | PartitionEnd _ ->
               Log.Global.error "End of partition";
               Deferred.return awaiting)
  in
  match String.Set.is_empty consumed with
  | true -> return ()
  | false -> Deferred.return @@ Error (Kafka.FAIL, "Not all messages consumed")

let main_or_error host port topic =
  match%map main_result host port topic with
  | Ok _ as ok -> ok
  | Error (_, msg) -> Error (Error.of_string msg)

let main () =
  let open Command.Let_syntax in
  Command.async_or_error
    ~summary:"Run the async integration test of the OCaml Kafka bindings"
    [%map_open
      let _ = Log.Global.set_level_via_param ()
      and host =
        flag "host"
          (optional_with_default "localhost" string)
          ~doc:"HOSTNAME Which broker to connect to"
      and port =
        flag "port"
          (optional_with_default 9092 int)
          ~doc:"PORTNUMBER Which port to connect to"
      and topic =
        flag "topic"
          (optional_with_default "test" string)
          ~doc:"TOPIC Which topic to use"
      in
      fun () -> main_or_error host port topic]
  |> Command.run

let () = main ()
