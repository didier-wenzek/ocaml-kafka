open Core
open Async

let main (brokers, topic, messages) =
  let open Deferred.Result.Let_syntax in
  Log.Global.debug "Starting";
  let%bind producer =
    Deferred.return
    @@ Kafka_async.new_producer [ ("metadata.broker.list", brokers) ]
  in
  Log.Global.debug "Got a producer";
  let%bind topic = Deferred.return @@ Kafka_async.new_topic producer topic [] in
  Log.Global.debug "Got a topic";
  let partition = 0 in
  let defs =
    List.map ~f:(Kafka_async.produce producer topic ~partition) messages
  in
  let%bind _ = Deferred.Result.all defs in
  Log.Global.info "Produced successfully";
  return ()

let main_or_error opts =
  match%bind main opts with
  | Ok _ as v -> return v
  | Error (_, msg) -> return @@ Error (Error.of_string msg)

let () =
  let open Command.Let_syntax in
  Command.async_or_error ~summary:"Produce messages on Kafka topic"
    [%map_open
      let _ = Log.Global.set_level_via_param ()
      and topic =
        flag "topic" (required string) ~doc:"NAME Which topic to post to"
      and msg, messages =
        anon (t2 ("payload" %: string) (sequence ("payload" %: string)))
      and brokers =
        flag "brokers"
          (optional_with_default "localhost:9092" string)
          ~doc:"BROKERS Comma separated list of brokers to connect to"
      in
      fun () -> main_or_error (brokers, topic, msg :: messages)]
  |> Command.run
