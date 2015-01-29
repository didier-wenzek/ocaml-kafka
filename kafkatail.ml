let main () =
  let brokers = Sys.argv.(1) in
  let topic = Sys.argv.(2) in
  let partition = int_of_string Sys.argv.(3) in
  let offset = Int64.of_string Sys.argv.(4) in
  let timout = 1000 in

  let consumer = Kafka.new_consumer ["metadata.broker.list",brokers] in
  let topic = Kafka.new_topic consumer topic [
    "auto.commit.enable","true";
    "offset.store.method","file";
    "auto.offset.reset","largest";
    "offset.store.path","."
  ] in

  let rec loop () = 
    match Kafka.consume topic partition timout with
    | Kafka.Message (_,_,offset,msg) -> (Printf.printf "%Ld: %s\n%!" offset msg;loop ())
    | Kafka.PartitionEnd (_,_,offset) -> Printf.printf "%Ld\n%!" offset
    | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (Printf.fprintf stderr "Timeout after: %d ms\n%!" timout; loop ())
    | exception Kafka.Error(_,msg) -> Printf.fprintf stderr "Error: %s\n%!" msg
  in

  Kafka.consume_start topic partition offset;
  loop ();
  Kafka.consume_stop topic partition;
  Kafka.store_offset topic partition offset;
  Kafka.destroy_topic topic;
  Kafka.destroy_handler consumer
 
in 
  if Array.length Sys.argv != 5
  then Printf.printf "usage: %s brokers topic partition offset\n%!" Sys.argv.(0)
  else main ()
     

