open Kafka.Metadata

let main () =
  let brokers = Sys.argv.(1) in
  let topic = Sys.argv.(2) in
  let partition = Sys.argv.(3) in
  let offset = Int64.of_string Sys.argv.(4) in
  let timout = 1000 in

  let consumer = Kafka.new_consumer ["metadata.broker.list",brokers] in
  let topic = Kafka.new_topic consumer topic [
    "auto.commit.enable","true";
    "offset.store.method","file";
    "auto.offset.reset","largest";
    "offset.store.path","."
  ] in
  let queue = Kafka.new_queue consumer in

  let (partitions,offsets) =
     try ([int_of_string partition],[offset])
     with e -> (
        let meta = Kafka.topic_metadata consumer topic in
        (meta.topic_partitions, List.map (fun _ -> offset) meta.topic_partitions)
     )
  in

  let rec loop () = 
    match Kafka.consume_queue queue timout with
    | Kafka.Message (topic,partition,offset,msg,None) -> (Printf.printf "%s,%d,%Ld: %s\n%!" (Kafka.topic_name topic) partition offset msg;loop ())
    | Kafka.Message (topic,partition,offset,msg,Some(key)) -> (Printf.printf "%s,%d,%Ld: %s->%s\n%!" (Kafka.topic_name topic) partition offset key msg;loop ())
    | Kafka.PartitionEnd (topic,partition,offset) -> (Printf.printf "%s,%d,%Ld\n%!" (Kafka.topic_name topic) partition offset; loop())
    | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (Printf.fprintf stderr "Timeout after: %d ms\n%!" timout; loop ())
    | exception Kafka.Error(_,msg) -> Printf.fprintf stderr "Error: %s\n%!" msg
  in

  List.iter2 (Kafka.consume_start_queue queue topic) partitions offsets;
  loop ();
  List.iter (Kafka.consume_stop topic) partitions;

  Kafka.destroy_topic topic;
  Kafka.destroy_queue queue;
  Kafka.destroy_handler consumer
 
let () = 
  if Array.length Sys.argv != 5
  then (
    Printf.printf "usage: %s brokers topic partition offset\n%!" Sys.argv.(0);
    Printf.printf "       %s brokers topic all offset\n%!" Sys.argv.(0)
  )
  else main ()
