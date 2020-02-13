open Core
open Async

let options =
  [
    ("metadata.broker.list", "localhost:9092");
    ("group.id", "test-consumer-groups");
    ("auto.offset.reset", "earliest");
  ]

let main topic =
  let open Deferred.Result.Let_syntax in
  Log.Global.debug "Starting";
  let%bind consumer = Deferred.return @@ Kafka_async.new_consumer options in
  Log.Global.debug "Got a consumer";
  let%bind reader = Deferred.return @@ Kafka_async.consume consumer ~topic in
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

let main_or_error topic =
  match%bind main topic with
  | Ok _ as v -> return v
  | Error (_, msg) -> return @@ Error (Error.of_string msg)

let () =
  let open Command.Let_syntax in
  Command.async_or_error ~summary:"Consume messages on Kafka topic"
    [%map_open
      let _ = Log.Global.set_level_via_param ()
      and topic =
        flag "topic" (required string) ~doc:"NAME Which topic to consume from"
      in
      fun () -> main_or_error topic]
  |> Command.run
