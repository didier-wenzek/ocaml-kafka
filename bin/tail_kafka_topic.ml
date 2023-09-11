open Cmdliner
open Lwt
open Kafka.Metadata

let info =
  let doc = "Echo all messages consumed from a kafka topic" in
  let man =
    [
      `S "DESCRIPTION";
      `P
        "$(tname) prints all messages consumed from the kafka BROKERS along \
         the topic TOPIC.";
      `P "All partitions are consumed, unless a specific PARTITION is given.";
      `P "Consumption starts from OFFSET or from the oldest messages.";
      `P
        "Messages are consumed by batch of MSG_COUNT messages,\n\
        \        waiting at most TIMEOUT_MS milli-seconds.";
    ]
  in
  Cmd.info "tail_kafka_topic" ~doc ~man

let print_msg = function
  | Kafka.Message (topic, partition, offset, msg, None) ->
      Lwt_io.printf "%s,%d,%Ld::%s\n%!" (Kafka.topic_name topic) partition
        offset msg
  | Kafka.Message (topic, partition, offset, msg, Some key) ->
      Lwt_io.printf "%s,%d,%Ld:%s:%s\n%!" (Kafka.topic_name topic) partition
        offset key msg
  | Kafka.PartitionEnd (topic, partition, offset) ->
      Lwt_io.printf "%s,%d,%Ld (EOP)\n%!" (Kafka.topic_name topic) partition
        offset

let tail_topic brokers msg_count timeout_ms topic_name partition offset =
  let consumer = Kafka.new_consumer [ ("metadata.broker.list", brokers) ] in
  let topic = Kafka.new_topic consumer topic_name [] in
  let queue = Kafka.new_queue consumer in
  let partitions =
    match partition with
    | Some p -> [ p ]
    | None -> (Kafka.topic_metadata consumer topic).topic_partitions
  in
  let start () =
    List.iter
      (fun partition -> Kafka.consume_start_queue queue topic partition offset)
      partitions
    |> return
  in
  let rec loop () =
    Kafka_lwt.consume_batch_queue ~timeout_ms ~msg_count queue
    >>= Lwt_list.iter_s print_msg >>= loop
  in
  let term () =
    Kafka.destroy_topic topic;
    Kafka.destroy_queue queue;
    Kafka.destroy_handler consumer;
    return ()
  in
  Lwt_main.run (start () >>= loop >>= term)

let brokers =
  let doc = "Kafka broker hosts (comma sepated list of 'host:port')" in
  Arg.(
    value & opt string "localhost"
    & info [ "b"; "brokers" ] ~docv:"BROKERS" ~doc)

let msg_count =
  let doc = "Count of messages to consume per batch." in
  Arg.(value & opt int 1024 & info [ "c"; "count" ] ~docv:"MSG_COUNT" ~doc)

let timeout_ms =
  let doc =
    "Wait at most $(docv) milli-seconds to fill a batch of the requested size."
  in
  Arg.(value & opt int 1000 & info [ "t"; "timeout" ] ~docv:"TIMEOUT_MS" ~doc)

let offset =
  let doc = "The offset to start with." in
  Arg.(value & opt int64 0L & info [ "o"; "offset" ] ~docv:"OFFSET" ~doc)

let topic =
  let doc = "The topic to consume." in
  Arg.(required & pos 0 (some string) None & info [] ~docv:"TOPIC" ~doc)

let partition =
  let doc = "The partition to consume." in
  Arg.(value & pos 1 (some int) None & info [] ~docv:"PARTITION" ~doc)

let tail_topic_t =
  Term.(
    const tail_topic $ brokers $ msg_count $ timeout_ms $ topic $ partition
    $ offset)

let () = Cmd.v info tail_topic_t |> Cmd.eval |> exit
