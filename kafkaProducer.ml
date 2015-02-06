type ('a,'b) sink = unit -> ('a -> unit) * (unit -> 'b)
type 'a iterable = ('a -> unit) -> unit

let stream_to open_sink iterable =
  let (push,close) = open_sink () in
  try iterable push ; close ()
  with error -> close (); raise error 
  
let partition_sink
  ?(producer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  topic_name partition
= fun () ->
  let producer = Kafka.new_producer producer_props in
  let topic = Kafka.new_topic producer topic_name topic_props in
  let push msg = Kafka.produce topic partition msg in
  let term () = (Kafka.destroy_topic topic; Kafka.destroy_handler producer) in
  (push,term)
