let usage prg =
  Printf.fprintf stderr "usage: %s topic-name\n" prg;
  Printf.fprintf stderr "\tCreate a topic on the local broker, using server default settings.\n";
  exit 1 |> ignore

open Kafka.Metadata

let create_topic topic =
  let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"] in
  let producer_topic = Kafka.new_topic producer topic ["message.timeout.ms","1000"] in

  let topic_info = Kafka.topic_metadata producer producer_topic in
  let partition_count = List.length topic_info.Kafka.Metadata.topic_partitions in
  if partition_count > 0
  then
    Printf.printf "Topic '%s' created with %d partitions.\n" topic partition_count
  else
    Printf.printf "Topic '%s' creation requested.\n" topic;
   
  Kafka.destroy_topic producer_topic;
  Kafka.destroy_handler producer

let () =
  if Array.length Sys.argv != 2
  then
    usage Sys.argv.(0)
  else
    let topic = Sys.argv.(1) in
    create_topic topic
