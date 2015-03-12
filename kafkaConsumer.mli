
val fold_partition:
     ?consumer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> ?timeout_ms:int
  -> ?stop_at_end:bool               (* stop when end of partition is reach ? default is false *)
  -> string -> int                   (* topic name and partition to consume *)
  -> ('a -> Kafka.message -> 'a)     (* function used to update the accumulator value with a message *)
  -> int64                           (* first offset to consume *)
  -> 'a                              (* seed accumulator value *)                      
  -> 'a                              (* final accumulated value *)                      

val fold_topic:
     ?consumer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> ?timeout_ms:int
  -> ?stop_at_end:bool                    (* stop when end of partition is reach, for all partitions. default is false *)
  -> string -> int list                   (* topic name and partitions to consume (all partitions of the topic if none is provided) *)
  -> ('a -> Kafka.message -> 'a)          (* function used to update the accumulator value with a message *)
  -> (int*int64) list                     (* first offset to consume for each partition *)
  -> 'a                                   (* seed accumulator value *)
  -> 'a                                   (* final accumulated value *)                      

val fold_queue:
     ?consumer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> ?timeout_ms:int
  -> ?stop_at_end:bool                    (* stop when end of partition is reach, for all partitions. default is false *)
  -> (string*int) list                    (* topic,partition pairs to consume *)
  -> ('a -> Kafka.message -> 'a)          (* function used to update the accumulator value with a message *)
  -> (string*int*int64) list              (* first offset to consume for each partition *)
  -> 'a                                   (* seed accumulator value *)
  -> 'a                                   (* final accumulated value *)                      
