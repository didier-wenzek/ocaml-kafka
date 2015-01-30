open Kafka.Metadata
let timeout_ms = 1000
let skip_all_message consume = 
  let rec loop () = match consume (3*timeout_ms) with
  | Kafka.Message _ -> loop ()
  | Kafka.PartitionEnd _ -> ()
  in loop ()

let main =

   (* Prepare a producer handler. *)
   let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"] in

   (* Check that there is a test topic with two partitions. *)
   let topics = Kafka.all_topics_metadata producer in
   let test = List.find (fun tm -> tm.topic_name = "test") topics in
   assert (List.sort compare test.topic_partitions = [0;1]);

   let producer_topic = Kafka.new_topic producer "test" ["message.timeout.ms","10000"] in
   let test = Kafka.topic_metadata producer producer_topic in
   assert (List.sort compare test.topic_partitions = [0;1]);

   (* Prepare a consumer handler *)
   let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"] in
   let consumer_topic = Kafka.new_topic consumer "test" ["auto.commit.enable","false"] in
   let partition = 1 in

   (* Start collecting messages *)
   (* Here we start from offset_end, i.e. we will consume only messages produced from now. *)
   Kafka.consume_start consumer_topic partition Kafka.offset_end;
   skip_all_message (Kafka.consume consumer_topic partition);

   (* Produce some messages *)
   Kafka.produce producer_topic partition "message 0";
   Kafka.produce producer_topic partition "message 1";
   Kafka.produce producer_topic partition "message 2";
   
   (* Consume messages *)
   let rec consume t p = match Kafka.consume t p timeout_ms with
      | Kafka.Message(_,_,_,msg) -> msg
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

   (match Kafka.consume consumer_topic partition timeout_ms with
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
   skip_all_message (Kafka.consume_queue queue);

   Kafka.produce producer_topic Kafka.partition_unassigned "message 3";
   Kafka.produce producer_topic Kafka.partition_unassigned "message 4";
   Kafka.produce producer_topic Kafka.partition_unassigned "message 5";

   let rec consume_queue (n,m) = match Kafka.consume_queue queue timeout_ms with
      | Kafka.Message(topic,partition,offset,msg) -> (
          assert (topic == consumer_topic);
          assert (partition = 0 || partition = 1);
          if partition = 0
          then (n+1,m)
          else (n,m+1)
      )
      | Kafka.PartitionEnd(_,_,_) -> (
          Printf.fprintf stderr "No message for now\n%!";
          consume_queue (n,m)
      )
      | exception Kafka.Error(Kafka.TIMED_OUT,_) -> (
          Printf.fprintf stderr "Timeout after: %d ms\n%!" timeout_ms;
          consume_queue (n,m)
      )
   in
   let (n,m) = consume_queue (0,0) in
   let (n,m) = consume_queue (n,m) in
   let (n,m) = consume_queue (n,m) in
   
   assert (n+m = 3);

   Kafka.consume_stop consumer_topic 0;
   Kafka.consume_stop consumer_topic 1;

   (* Consumers, producers, topics and queues, all handles must be released. *)
   Kafka.destroy_queue queue;
   Kafka.destroy_topic producer_topic;
   Kafka.destroy_topic consumer_topic;
   Kafka.destroy_handler producer;
   Kafka.destroy_handler consumer


