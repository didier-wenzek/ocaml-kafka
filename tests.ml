let main =

   (* Prepare a producer handler. *)
   let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"] in
   let producer_topic = Kafka.new_topic producer "test" ["message.timeout.ms","10000"] in

   (* Prepare a consumer handler *)
   let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"] in
   let consumer_topic = Kafka.new_topic consumer "test" ["auto.commit.enable","false"] in
   let partition = 1 in
   let timeout_ms = 1000 in

   (* Start collecting messages *)
   (* Here we start from offset_end, i.e. we will consume only messages produced from now. *)
   Kafka.consume_start consumer_topic partition Kafka.offset_end;

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

   (* Topics and producers must be released. *)
   Kafka.destroy_topic producer_topic;
   Kafka.destroy_handler producer;
   Kafka.destroy_topic consumer_topic;
   Kafka.destroy_handler consumer


