open Kafka.Metadata

(** [protect ~finally f x] calls [f x] and ensures that [finally ()] is called before returning [f x].

    Adapted from http://stackoverflow.com/questions/11276985/emulating-try-with-finally-in-ocaml.
*)
let protect ~finally f x =
  let module E = struct type 'a t = Left of 'a | Right of exn end in
  let res = try E.Left (f x) with e -> E.Right e in
  let () = finally () in
  match res with
  | E.Left  r -> r
  | E.Right e -> raise e

let fold_partition
  ?(consumer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(timeout_ms = 1000)
  ?(stop_at_end = false)
  topic_name partition update offset seed
=
  let consumer = Kafka.new_consumer consumer_props in
  let topic = Kafka.new_topic consumer topic_name topic_props in
  let start_consuming () =
    Kafka.consume_start topic partition offset
  in
  let stop_consuming () =
    Kafka.consume_stop topic partition;
    Kafka.destroy_topic topic;
    Kafka.destroy_handler consumer;
  in
  let rec loop acc =
    match Kafka.consume ~timeout_ms topic partition with
    | Kafka.Message _ as msg ->
      loop (update acc msg)
    | Kafka.PartitionEnd _ as msg ->
      let acc = update acc msg in
      if stop_at_end
      then acc
      else loop acc
    | exception Kafka.Error(Kafka.TIMED_OUT,_) ->
      if stop_at_end
      then acc
      else loop acc
    | exception e ->
      raise e
  in
  start_consuming ();
  protect ~finally:stop_consuming loop seed

let fold_queue_for_ever queue timeout_ms update seed
=
  let rec loop acc =
    match Kafka.consume_queue ~timeout_ms queue with
    | Kafka.Message _ as msg ->
      loop (update acc msg)
    | Kafka.PartitionEnd _ as msg ->
      loop (update acc msg)
    | exception Kafka.Error(Kafka.TIMED_OUT,_) ->
      loop acc
    | exception e -> 
      raise e
    in
    loop seed

module Partition = struct
  type t = int
  let compare a b = a - b
end
module PartitionSet : Set.S with type elt = int = Set.Make(Partition)

let fold_queue_upto_end queue timeout_ms update partitions seed
=
  let rec loop (partition_set,acc) =
    match Kafka.consume_queue ~timeout_ms queue with
    | Kafka.Message (_,partition,_,_,_) as msg ->
      let partition_set = PartitionSet.add partition partition_set in
      loop (partition_set, update acc msg)
    | Kafka.PartitionEnd (_,partition,_) as msg ->
      let partition_set = PartitionSet.remove partition partition_set in
      let acc = update acc msg in
      if PartitionSet.is_empty partition_set
      then acc
      else loop (partition_set,acc)
    | exception Kafka.Error(Kafka.TIMED_OUT,_) ->
      loop (partition_set,acc)
    | exception e -> (
      raise e
    )
    in
    loop (PartitionSet.of_list partitions,seed)

let find_offset partition_offsets partition =
  try List.assoc partition partition_offsets
  with Not_found -> 0L

let fold_topic
  ?(consumer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(timeout_ms = 1000)
  ?(stop_at_end = false)
  topic_name partitions update partition_offsets seed
=
  let consumer = Kafka.new_consumer consumer_props in
  let topic = Kafka.new_topic consumer topic_name topic_props in
  let partitions = match partitions with
    | [] -> (Kafka.topic_metadata consumer topic).topic_partitions
    | _ -> partitions
  in
  let offsets = List.map (find_offset partition_offsets) partitions in
  let queue = Kafka.new_queue consumer in
  let start_consuming () =
    List.iter2 (Kafka.consume_start_queue queue topic) partitions offsets
  in
  let loop seed =
    if stop_at_end
    then fold_queue_upto_end queue timeout_ms update partitions seed
    else fold_queue_for_ever queue timeout_ms update seed;
  in
  let stop_consuming () =
    List.iter (Kafka.consume_stop topic) partitions;
    Kafka.destroy_queue queue;
    Kafka.destroy_topic topic;
    Kafka.destroy_handler consumer;
  in
  start_consuming ();
  protect ~finally:stop_consuming loop seed

module TopicMap : Map.S with type key = string = Map.Make(String)

let fold_queue
  ?(consumer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(timeout_ms = 1000)
  ?(stop_at_end = false)
  topic_partition_pairs update topic_partition_offsets seed
=
  let consumer = Kafka.new_consumer consumer_props in
  let topics = List.fold_left (fun acc (topic_name,_) ->
    if TopicMap.mem topic_name acc
    then acc
    else TopicMap.add topic_name (Kafka.new_topic consumer topic_name topic_props) acc
  ) TopicMap.empty topic_partition_pairs in
  let partitions = List.map (fun (topic_name,partition) ->
    (TopicMap.find topic_name topics, partition)
  ) topic_partition_pairs in
  let offsets = List.map (fun (topic_name,partition,offset) ->
    (TopicMap.find topic_name topics, partition,offset)
  ) topic_partition_offsets in
  let queue = Kafka.new_queue consumer in
  let start_consuming () =
    List.iter (fun (topic,partition,offset) -> Kafka.consume_start_queue queue topic partition offset) offsets;
  in
  let loop seed =
    fold_queue_for_ever queue timeout_ms update seed;
  in
  let stop_consuming () =
    List.iter (fun (topic,partition) -> Kafka.consume_stop topic partition) partitions;
    Kafka.destroy_queue queue;
    TopicMap.iter (fun _ topic -> Kafka.destroy_topic topic) topics;
    Kafka.destroy_handler consumer;
  in
  start_consuming ();
  protect ~finally:stop_consuming loop seed
