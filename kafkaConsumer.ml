open Kafka.Metadata

let fold_partition
  ?(consumer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(timeout_ms = 1000)
  ?(stop_at_end = true)
  topic_name partition update offset seed
=
  let consumer = Kafka.new_consumer consumer_props in
  let topic = Kafka.new_topic consumer topic_name topic_props in
  let start_consuming () =
    Kafka.consume_start topic partition offset
  in
  let stop_consuming result =
    Kafka.consume_stop topic partition;
    Kafka.destroy_topic topic;
    Kafka.destroy_handler consumer;
    result
  in
  let rec loop acc =
    match Kafka.consume topic partition timeout_ms with
    | Kafka.Message (_,_,offset,msg,_) ->
      loop (update acc offset msg)
    | Kafka.PartitionEnd (_,_,_) ->
      if stop_at_end
      then stop_consuming acc
      else loop acc
    | exception Kafka.Error(Kafka.TIMED_OUT,_) ->
      if stop_at_end
      then stop_consuming acc
      else loop acc
    | exception e -> (
      stop_consuming acc;
      raise e
    )
  in
  start_consuming ();
  loop seed

module Partition = struct
  type t = int
  let compare a b = a - b
end
module PartitionSet : Set.S with type elt = int = Set.Make(Partition)

let find_offset partition_offsets partition =
  try List.assoc partition partition_offsets
  with Not_found -> 0L

let fold_topic
  ?(consumer_props = ["metadata.broker.list","localhost:9092"])
  ?(topic_props = [])
  ?(timeout_ms = 1000)
  ?(stop_at_end = true)
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
  let stop_consuming result =
    List.iter (Kafka.consume_stop topic) partitions;
    Kafka.destroy_queue queue;
    Kafka.destroy_topic topic;
    Kafka.destroy_handler consumer;
    result
  in
  let rec loop (partition_set,acc) =
    match Kafka.consume_queue queue timeout_ms with
    | Kafka.Message (_,partition,offset,msg,_) ->
      let partition_set = PartitionSet.add partition partition_set in
      loop (partition_set, update acc (partition,offset) msg)
    | Kafka.PartitionEnd (_,partition,_) ->
      let partition_set = PartitionSet.remove partition partition_set in
      if stop_at_end && (PartitionSet.is_empty partition_set)
      then stop_consuming acc
      else loop (partition_set,acc)
    | exception Kafka.Error(Kafka.TIMED_OUT,_) ->
      if stop_at_end
      then stop_consuming acc
      else loop (partition_set,acc)
    | exception e -> (
      stop_consuming acc;
      raise e
    )
    in
    start_consuming ();
    loop (PartitionSet.of_list partitions,seed)
