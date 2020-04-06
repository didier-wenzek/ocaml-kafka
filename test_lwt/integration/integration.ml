open Lwt.Infix
let return = Lwt.return

let produce brokers topic_name keyed_messages =
  let producer = Kafka_lwt.new_producer ["metadata.broker.list",brokers] in
  let topic = Kafka.new_topic producer topic_name [] in
  let send (key,msg) =
    Kafka_lwt.produce topic Kafka.partition_unassigned ~key msg
  in
  let loop () =
    Lwt_list.iter_s send keyed_messages
  in
  let term () =
    Kafka_lwt.wait_delivery producer
    >>= fun () ->
    Kafka.destroy_topic topic;
(*    Kafka.destroy_handler producer;*)
    return ()
  in
  loop () >>= term

module Cache = Map.Make(String)

let cache c (k,v) = Cache.add k v c

let consume brokers topic_name messages =
  let expected_messages = messages |> List.fold_left cache Cache.empty |> ref in
  let remove_received = function
    | Kafka.Message (_,_,_,_,Some(key)) when Cache.mem key !expected_messages ->
      expected_messages := Cache.remove key !expected_messages
    | _ -> ()
  in
  let consumer = Kafka.new_consumer ["metadata.broker.list",brokers] in
  let topic = Kafka.new_topic consumer topic_name [] in
  let queue = Kafka.new_queue consumer in
  let partitions = (Kafka.topic_metadata consumer topic).topic_partitions in
  let start_partition partition =
    Kafka.consume_start_queue queue topic partition Kafka.offset_beginning
  in
  let start () =
    partitions
    |> List.iter start_partition
    |> return
  in
  let rec loop () = 
    if Cache.is_empty !expected_messages
    then
      return ()
    else
      Kafka_lwt.consume_queue queue
      >|=
      remove_received
      >>=
      loop
  in
  let term () =
    Kafka.destroy_queue queue;
    Kafka.destroy_topic topic;
    Kafka.destroy_handler consumer;
    return ()
  in
  start () >>= loop >>= term

let timeout s err =
  Lwt_unix.sleep s
  >>= fun () ->
  Lwt_io.printf "%s\n%!" err
  >>= fun () ->
  Lwt.fail Stdlib.Exit

let test_produce_consume () =
  let brokers = "localhost" in
  let topic_name = "test" in
  let random_str prefix = prefix ^ (string_of_int (Random.int 1000000)) in
  let random_msg () = (random_str "k_", random_str "msg_") in
  let messages = [random_msg (); random_msg (); random_msg (); random_msg (); random_msg ()] in
  Lwt.join [
    produce brokers topic_name messages;
    Lwt.pick [
      consume brokers topic_name messages;
      timeout 2.0 "Fail to consume the expected messages."
    ]
  ]

let () =
  Lwt_main.run (test_produce_consume ())
