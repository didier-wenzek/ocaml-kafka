let usage () =
  Printf.fprintf stderr "These tests require a 'test' topic to be created with 2 partitions on local broker.\n";
  Printf.fprintf stderr " => abort tests\n%!";
  Printf.fprintf stderr "\n%!";
  Printf.fprintf stderr "You may use the following command to create the test topic:\n%!";
  Printf.fprintf stderr "$ $KAFKA_HOME/bin/kafka-topics.sh --create --topic test --partitions 2 --replication-factor 1 --zookeeper localhost\n%!";
  exit 1 |> ignore

open Kafka.Metadata
let timeout_ms = 1000

let skip_all_message consume partition = 
  let rec loop () = match consume partition with
  | Kafka.Message _ -> print_endline "garbage removed!";loop ()
  | Kafka.PartitionEnd _ -> ()
  | exception Kafka.Error(Kafka.TIMED_OUT,_) -> ()
  in loop ()

let main =

   Format.printf "Start sync tests using librdkafka version %s\n%!" (Kafka.librdkafka_version);

   (* Prepare a producer handler. *)
   let producer = Kafka.new_producer [
     "metadata.broker.list","localhost:9092";
     "queue.buffering.max.ms","1";
   ] in

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
   let consumer = Kafka.new_consumer [
     "metadata.broker.list","localhost:9092";
     "fetch.wait.max.ms", "10";
     "enable.partition.eof", "true";
   ] in
   let consumer_topic = Kafka.new_topic consumer "test" [
     "auto.commit.enable","false";
   ] in
   let partition = 1 in

   (* Start collecting messages *)
   (* Here we start from offset_end, i.e. we will consume only messages produced from now. *)
   Kafka.consume_start consumer_topic partition Kafka.offset_end;
   skip_all_message (Kafka.consume ~timeout_ms consumer_topic) partition;

   (* Produce some messages *)
   Kafka.produce producer_topic ~partition "message 0";
   Kafka.produce producer_topic ~partition "message 1";
   Kafka.produce producer_topic ~partition "message 2";
   
   (* Consume messages *)
   let rec consume t p = match Kafka.consume ~timeout_ms t p with
      | Kafka.Message(_,_,_,msg,_) -> msg
      | Kafka.PartitionEnd(_,_,_) -> (
          (* Printf.fprintf stderr "No message for now\n%!"; *)
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
     | Kafka.PartitionEnd (t,p,_) -> (
        assert (Kafka.topic_name t = "test");
        assert (p = partition)
     )
     | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (
          (* enable.partition.eof must be set to true to catch partition end *)
          assert false
      )
     | _ -> assert false
   );

   (* Message may be consumed by batch too *)
   Kafka.produce producer_topic ~partition "message 0 bis";
   Kafka.produce producer_topic ~partition "message 1 bis";
   Kafka.produce producer_topic ~partition "message 2 bis";

   let messages = Kafka.consume_batch ~timeout_ms:3000 ~msg_count:3 consumer_topic partition in
   assert (List.fold_left (fun acc -> function | Kafka.Message(_,_,_,msg,_) -> acc @ [msg] | _ -> acc) [] messages
        = ["message 0 bis"; "message 1 bis"; "message 2 bis"]);

   (* Stop collecting messages. *)
   Kafka.consume_stop consumer_topic partition;

   (* Use queues to collect messages from multi topics and partitions. *)
   let queue = Kafka.new_queue consumer in
   Kafka.consume_start_queue queue consumer_topic 0 Kafka.offset_end;
   Kafka.consume_start_queue queue consumer_topic 1 Kafka.offset_end;
   skip_all_message Kafka.consume_queue queue;

   Kafka.produce producer_topic "message 3";
   Kafka.produce producer_topic "message 4";
   Kafka.produce producer_topic "message 5";

   let rec consume_queue (n,m) = match Kafka.consume_queue ~timeout_ms queue with
      | Kafka.Message(topic,partition,_,_,_) -> (
          assert (topic == consumer_topic);
          assert (partition = 0 || partition = 1);
          if partition = 0
          then (n+1,m)
          else (n,m+1)
      )
      | Kafka.PartitionEnd(_,_,_) -> (
          (* Printf.fprintf stderr "No queued message for now\n%!"; *)
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

   (* Consuming batches of a queue. *)
   (* Here, we send 3 messages but we expect 5 messages: the 2 other messages are the partition ends. *)
   Kafka.produce producer_topic "message 0 ter";
   Kafka.produce producer_topic "message 1 ter";
   Kafka.produce producer_topic "message 2 ter";

   let messages = Kafka.consume_batch_queue ~timeout_ms:3000 ~msg_count:5 queue in
   assert (List.sort compare (List.fold_left (fun acc -> function
     | Kafka.Message(_,_,_,msg,_) -> Format.printf "Consume_batch_queue received: %s\n%!" msg; acc @ [msg]
     | Kafka.PartitionEnd(_,partition,_) -> Format.printf "Consume_batch_queue eof: %d\n%!" partition; acc
   ) [] messages) = ["message 0 ter"; "message 1 ter"; "message 2 ter"]);

   (* Using a partitioner to produce messages on a partition computed after the messages' keys. *)
   let partitioner_callback partition_cnt key = Some ((Hashtbl.hash key) mod partition_cnt) in
   let partitioner_topic = Kafka.new_topic ~partitioner_callback producer "test" [
     "message.timeout.ms","1000";
     "partitioner", "murmur2"
   ] in

   (* Produce some keyed messages *)
   let key_msg_pairs = List.map (fun k -> (k,"message "^k)) [""; "0"; "11"; "222"; "a"; "bb"; "ccc" ] in
   key_msg_pairs |> List.iter (fun (key,msg) -> Kafka.produce partitioner_topic ~key msg);
   
   (* Consume the keyed messages, checking they have been received on the right partition *)
   let rec consume_k t = match Kafka.consume_queue t with
      | Kafka.Message(_,_partition,_,msg,Some key) ->
          (* assert (partition = (partitioner_callback 2 key |> Option.get)); -- issue #26 *)
          key,msg
      | Kafka.Message(_,_,_,_,None) | Kafka.PartitionEnd(_,_,_) -> (
          consume_k t
      )
      | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (
          Printf.fprintf stderr "Timeout after: %d ms\n%!" timeout_ms;
          consume_k t
      )
   in
   let received_key_msg_pairs = key_msg_pairs |> List.map (fun _ -> consume_k queue) |> List.sort (fun p1 p2 -> compare (snd p1) (snd p2)) in
   assert (key_msg_pairs = received_key_msg_pairs);

   Kafka.consume_stop consumer_topic 0;
   Kafka.consume_stop consumer_topic 1;

   (* A callback may be attached to a producer to be called on message delivery. *)
   let last_msg_id = ref 0 in
   let last_error = ref None in

   let producer_with_delivery_callback =
       let delivery_callback msg_id err = (last_msg_id := msg_id; last_error := err) in
       Kafka.new_producer ~delivery_callback ["metadata.broker.list","localhost:9092"; "queue.buffering.max.ms","1"]
   in
   Gc.full_major (); (* The callback is properly registered as GC root *)
   let topic_with_delivery_callback = Kafka.new_topic producer_with_delivery_callback "test" ["message.timeout.ms","10000"] in
   Kafka.produce topic_with_delivery_callback ~msg_id:156 "message 6";
   Kafka.poll_events producer_with_delivery_callback |> ignore;
   assert (!last_msg_id = 156);
   assert (!last_error = None);

   (* Consumers, producers, topics and queues, all handles must be released. *)
   Kafka.destroy_queue queue;
   Kafka.destroy_topic producer_topic;
   Kafka.destroy_topic partitioner_topic;
   Kafka.destroy_topic consumer_topic;
   Kafka.destroy_handler producer;
   Kafka.destroy_handler consumer;

   (* KafkaConsumer / KafkaProducer API *)
   let open Kafka_helpers in
   let stop_at_end = true in
   let iterable_of_list xs f = List.iter f xs in
   let sink = Kafka_producer.partition_sink "test" ~partition:None in
   let src = Kafka_consumer.fold_topic ~stop_at_end "test" [] in
   let offsets = [0,Kafka.offset_tail 3; 1,Kafka.offset_tail 3] in
   
   ["message 123"; "message 124"; "message 125"] |> iterable_of_list |> Kafka_producer.stream_to sink;
   let messages = src (fun acc -> function Kafka.Message(_,_,_,msg,_) -> msg::acc | _ -> acc) offsets [] in
   assert (List.length messages = 6);
   assert (List.exists (fun msg -> msg = "message 123") messages);
   assert (List.exists (fun msg -> msg = "message 124") messages);
   assert (List.exists (fun msg -> msg = "message 125") messages);

   Format.printf "%s\n%!" "Done Sync tests";
   "Tests successful\n%!"
