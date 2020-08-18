type ('a,'b) sink = unit -> ('a -> unit) * (unit -> 'b)
type 'a iterable = ('a -> unit) -> unit

let stream_to : ('a, 'b) sink -> 'a iterable -> 'b = fun open_sink iterable ->
  let (push,close) = open_sink () in
  try iterable push ; close ()
  with error -> ignore (close ()); raise error
  
type 'a push_error_handler = ('a -> unit) -> 'a -> exn -> unit
let retry_on_error push msg _error = push msg
let raise_on_error _push _msg error = raise error

let partition_sink
  ?(producer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(delivery_error_handler = raise_on_error)
  topic_name partition
= fun () ->
  let producer = Kafka.new_producer producer_props in
  let topic = Kafka.new_topic producer topic_name topic_props in
  let rec push msg =
    let wait_and_push msg =
       let max_outq_len = ((Kafka.outq_len producer) * 4)/5 in
       Kafka.wait_delivery ~max_outq_len producer;
       push msg
    in
    try Kafka.produce topic partition msg
    with error -> delivery_error_handler wait_and_push msg error
  in
  let term () = (Kafka.wait_delivery producer; Kafka.destroy_topic topic; Kafka.destroy_handler producer) in
  (push,term)

let topic_sink
  ?(producer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(delivery_error_handler = raise_on_error)
  topic_name
= fun () ->
  let producer = Kafka.new_producer producer_props in
  let topic = Kafka.new_topic producer topic_name topic_props in
  let partition_count = List.length (Kafka.topic_metadata producer topic).Kafka.Metadata.topic_partitions in
  let rec push (partition,msg) =
    let wait_and_push p_msg =
       let max_outq_len = ((Kafka.outq_len producer) * 4)/5 in
       Kafka.wait_delivery ~max_outq_len producer;
       push p_msg
    in
    try Kafka.produce topic (Kafka.Assigned (partition mod partition_count)) msg
    with error -> delivery_error_handler wait_and_push (partition,msg) error
  in
  let term () = (Kafka.wait_delivery producer; Kafka.destroy_topic topic; Kafka.destroy_handler producer) in
  (push,term)

  
