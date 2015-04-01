let usage () =
  Printf.fprintf stderr "These tests require a 'test' topic to be created with 2 partitions on local broker.\n";
  Printf.fprintf stderr " => abort tests\n%!";
  Printf.fprintf stderr "\n%!";
  Printf.fprintf stderr "You may use:\n%!";
  Printf.fprintf stderr "$ make tools\n%!";
  Printf.fprintf stderr "$ ./create_topic.native test\n%!";
  exit 1 |> ignore

open Kafka.Metadata
let timeout_ms = 1000

let skip_all_message consume partition = 
  let rec loop () = match consume partition with
  | Kafka.Message _ -> loop ()
  | Kafka.PartitionEnd _ -> ()
  | exception Kafka.Error(Kafka.TIMED_OUT,_) -> ()
  in loop ()

let main =

   (* Prepare a producer handler. *)
   let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"] in

   (* Check that there is a test topic with two partitions. *)
   let () =
     try 
       let topics = Kafka.all_topics_metadata producer in
       let test = List.find (fun tm -> tm.topic_name = "test") topics in
       let partitions = List.sort compare test.topic_partitions in
       if partitions = [0;1] then () else usage ()
     with Not_found -> usage ()
   in

   (* Prepare a producer topic *)
   let producer_topic = Kafka.new_topic producer "test" ["message.timeout.ms","1000"] in
   let test = Kafka.topic_metadata producer producer_topic in
   assert (List.sort compare test.topic_partitions = [0;1]);

   (* Prepare a consumer handler *)
   let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"] in
   let consumer_topic = Kafka.new_topic consumer "test" ["auto.commit.enable","false"] in
   let partition = 1 in

   (* Start collecting messages *)
   (* Here we start from offset_end, i.e. we will consume only messages produced from now. *)
   Kafka.consume_start consumer_topic partition Kafka.offset_end;
   skip_all_message (Kafka.consume ~timeout_ms consumer_topic) partition;

   (* Produce some messages *)
   Kafka.produce producer_topic partition "message 0";
   Kafka.produce producer_topic partition "message 1";
   Kafka.produce producer_topic partition "message 2";
   
   (* Consume messages *)
   let rec consume t p = match Kafka.consume ~timeout_ms t p with
      | Kafka.Message(_,_,_,msg,_) -> msg
      | Kafka.PartitionEnd(_,_,_) -> (
          Printf.fprintf stderr "No message for now\n%!";
          consume t p
      )
      | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (
          Printf.fprintf stderr "Timeout after: %d ms\n%!" timeout_ms;
          consume t p
      )
   in
   let msg = consume consumer_topic partition in assert (msg = "message 0");
   let msg = consume consumer_topic partition in assert (msg = "message 1");
   let msg = consume consumer_topic partition in assert (msg = "message 2");

   (match Kafka.consume ~timeout_ms consumer_topic partition with
     | Kafka.PartitionEnd (t,p,o) -> (
        assert (Kafka.topic_name t = "test");
        assert (p = partition)
     )
     | _ -> assert false
   );

   (* Stop collecting messages. *)
   Kafka.consume_stop consumer_topic partition;

   (* Use queues to collect messages from multi topics and partitions. *)
   let queue = Kafka.new_queue consumer in
   Kafka.consume_start_queue queue consumer_topic 0 Kafka.offset_end;
   Kafka.consume_start_queue queue consumer_topic 1 Kafka.offset_end;
   skip_all_message Kafka.consume_queue queue;

   Kafka.produce producer_topic Kafka.partition_unassigned "message 3";
   Kafka.produce producer_topic Kafka.partition_unassigned "message 4";
   Kafka.produce producer_topic Kafka.partition_unassigned "message 5";

   let rec consume_queue (n,m) = match Kafka.consume_queue ~timeout_ms queue with
      | Kafka.Message(topic,partition,offset,msg,_) -> (
          assert (topic == consumer_topic);
          assert (partition = 0 || partition = 1);
          if partition = 0
          then (n+1,m)
          else (n,m+1)
      )
      | Kafka.PartitionEnd(_,_,_) -> (
          Printf.fprintf stderr "No queued message for now\n%!";
          consume_queue (n,m)
      )
      | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (
          Printf.fprintf stderr "Queue timeout after: %d ms\n%!" timeout_ms;
          consume_queue (n,m)
      )
   in
   let (n,m) = consume_queue (0,0) in
   let (n,m) = consume_queue (n,m) in
   let (n,m) = consume_queue (n,m) in
   
   assert (n+m = 3);

   (* Produce some keyed messages *)
   let partitioner_callback partition_cnt key = Printf.printf "xoxox %s \n%!" key;(Hashtbl.hash key) mod partition_cnt in
   let keyed_topic = Kafka.new_topic ~partitioner_callback producer "test" ["message.timeout.ms","1000"] in
   Kafka.produce keyed_topic Kafka.partition_unassigned ~key:"key 0" "key-message 0";
   Kafka.produce keyed_topic Kafka.partition_unassigned ~key:"key 1" "key-message 1";
   Kafka.produce keyed_topic Kafka.partition_unassigned ~key:"key 2" "key-message 2";
   
   (* Consume keyed messages *)
   let rec consume_k t = match Kafka.consume_queue t with
      | Kafka.Message(_,_,_,msg,key) -> key,msg
      | Kafka.PartitionEnd(_,_,_) -> (
          Printf.fprintf stderr "No keyed message for now\n%!";
          consume_k t
      )
      | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (
          Printf.fprintf stderr "Timeout after: %d ms\n%!" timeout_ms;
          consume_k t
      )
   in
   let key_msg_pairs = [ consume_k queue; consume_k queue; consume_k queue ] in
   let key_msg_pairs = List.sort (fun p1 p2 -> compare (snd p1) (snd p2)) key_msg_pairs in
   assert (key_msg_pairs = [Some "key 0","key-message 0"; Some "key 1","key-message 1"; Some "key 2","key-message 2"]);

   Kafka.consume_stop consumer_topic 0;
   Kafka.consume_stop consumer_topic 1;

   (* A callback may be attached to a producer to be called on message delivery. *)
   let last_message_produced = ref "" in
   let last_msg_id = ref None in
   let last_error = ref None in
   let delivery_callback msg msg_id err = (last_message_produced := msg; last_msg_id := msg_id; last_error := err) in

   let producer_with_delivery_callback = Kafka.new_producer ~delivery_callback ["metadata.broker.list","localhost:9092"] in
   let topic_with_delivery_callback = Kafka.new_topic producer_with_delivery_callback "test" ["message.timeout.ms","10000"] in
   Kafka.produce topic_with_delivery_callback Kafka.partition_unassigned ~msg_id:156L "message 6"; 
   Kafka.poll_events producer_with_delivery_callback |> ignore;
   assert (!last_message_produced = "message 6");
   assert (!last_msg_id = Some 156L);
   assert (!last_error = None);

   (* Consumers, producers, topics and queues, all handles must be released. *)
   Kafka.destroy_queue queue;
   Kafka.destroy_topic producer_topic;
   Kafka.destroy_topic keyed_topic;
   Kafka.destroy_topic consumer_topic;
   Kafka.destroy_handler producer;
   Kafka.destroy_handler consumer;

   (* KafkaConsumer / KafkaProducer API *)
   let stop_at_end = true in
   let iterable_of_list xs f = List.iter f xs in
   let sink = KafkaProducer.partition_sink "test" Kafka.partition_unassigned in
   let src = KafkaConsumer.fold_topic ~stop_at_end "test" [] in
   let offsets = [0,Kafka.offset_tail 3; 1,Kafka.offset_tail 3] in
   
   ["message 123"; "message 124"; "message 125"] |> iterable_of_list |> KafkaProducer.stream_to sink;
   let messages = src (fun acc -> function Kafka.Message(_,_,_,msg,_) -> msg::acc | _ -> acc) offsets [] in
   assert (List.length messages = 6);
   assert (List.exists (fun msg -> msg = "message 123") messages);
   assert (List.exists (fun msg -> msg = "message 124") messages);
   assert (List.exists (fun msg -> msg = "message 125") messages)


   
   
   

