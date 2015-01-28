let main =

   (* Prepare a producer handler. *)
   let producer = Kafka.new_producer ["metadata.broker.list","localhost:9092"] in
   let producer_topic = Kafka.new_topic producer "test" in

   (* Prepare a consumer handler *)
   let consumer = Kafka.new_consumer ["metadata.broker.list","localhost:9092"] in
   let consumer_topic = Kafka.new_topic consumer "test" in
   let partition = 1 in

   (* Start collecting messages *)
   (* Here we start from offset_end, i.e. we will consume only messages produced from now. *)
   Kafka.consume_start consumer_topic partition Kafka.offset_end;

   (* Produce some messages *)
   Kafka.produce producer_topic partition "message 0";
   Kafka.produce producer_topic partition "message 1";
   Kafka.produce producer_topic partition "message 2";
   
   (* Consume messages *)
   let timeout_ms = 1000 in
   let (off0,msg) = Kafka.consume consumer_topic partition timeout_ms in
   assert (msg = "message 0");
   let (off1,msg) = Kafka.consume consumer_topic partition timeout_ms in
   assert (msg = "message 1");
   let (off2,msg) = Kafka.consume consumer_topic partition timeout_ms in
   assert (msg = "message 2");

   (* Stop collecting messages. *)
   Kafka.consume_stop consumer_topic partition;

   (* Topics and producers must be released. *)
   Kafka.destroy_topic producer_topic;
   Kafka.destroy_handler producer;
   Kafka.destroy_topic consumer_topic;
   Kafka.destroy_handler consumer


