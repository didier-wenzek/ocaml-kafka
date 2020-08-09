open Core
open Async

let options = [ ("auto.offset.reset", "earliest") ]

let main (brokers, (topic, topics), group_id) =
  let open Deferred.Result.Let_syntax in
  Log.Global.debug "Starting";
  let options =
    ("metadata.broker.list", brokers) :: ("group.id", group_id) :: options
  in
  let%bind consumer = Deferred.return @@ Kafka_async.new_consumer options in
  Log.Global.debug "Got a consumer";
  let topics = topic :: topics in
  let%bind readers =
    topics
    |> List.map ~f:(fun topic ->
    Kafka_async.consume consumer ~topic)
    |> Result.all
    |> Deferred.return
  in
  Log.Global.debug "Set up subscriptions on %d topics" (List.length topics);
  let reader = Pipe.interleave readers in
  Log.Global.debug "Merged subscriptions";
  let%bind () =
    Deferred.ok
    @@ Pipe.iter reader ~f:(function
         | Message (topic, _, _, payload, _) ->
             let topic_name = Kafka.topic_name topic in
             Deferred.return
             @@ Log.Global.info "Message on topic '%s', payload '%s'" topic_name
                  payload
         | PartitionEnd _ ->
             Deferred.return @@ Log.Global.error "End of partition")
  in
  Log.Global.debug "Consumed";
  Kafka_async.destroy_consumer consumer;
  Log.Global.debug "Destroyed";
  return ()

let main_or_error opts =
  match%bind main opts with
  | Ok _ as v -> return v
  | Error (_, msg) -> return @@ Error (Error.of_string msg)

let () =
  let open Command.Let_syntax in
  Command.async_or_error ~summary:"Consume messages on Kafka topic"
    [%map_open
      let _ = Log.Global.set_level_via_param ()
      and topics =
        flag "topic" (one_or_more string) ~doc:"NAME Which topics to consume from"
      and group_id =
        flag "group-id"
          (optional_with_default "ocaml-kafka-async-consumer" string)
          ~doc:"GROUPID Which group.id to pick for consuming"
      and brokers =
        flag "brokers"
          (optional_with_default "localhost:9092" string)
          ~doc:"BROKERS Comma separated list of brokers to connect to"
      in
      fun () -> main_or_error (brokers, topics, group_id)]
  |> Command.run
