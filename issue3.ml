open Lwt

let rec count_message topic partition =
   Kafka_lwt.consume_batch topic partition
   >>= fun messages ->
   Lwt_io.printl (string_of_int (List.length messages))
   >>= fun () ->
   count_message topic partition

let main =
   let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"] in
   let topic = Kafka.new_topic consumer "test" [] in

   Kafka.consume_start topic 0 Kafka.offset_end;
   Kafka.consume_start topic 1 Kafka.offset_end;

   Lwt.join [
      count_message topic 0;
      count_message topic 1
   ]

let _ =
  Lwt_main.run main
