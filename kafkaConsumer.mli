
val fold_partition:
     ?consumer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> ?timeout_ms:int
  -> ?stop_at_end:bool
  -> string -> int                   (* topic name and partition to consume *)
  -> ('a -> int64 -> string -> 'a)   (* function used to update an accumulator value of type 'a with an offset and a message *)
  -> int64                           (* first offset to consume *)
  -> 'a                              (* seed accumulator value *)                      
  -> 'a                              (* final accumulated value *)                      

val fold_topic:
     ?consumer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> ?timeout_ms:int
  -> ?stop_at_end:bool
  -> string -> int list                   (* topic name and partitions to consume (all partitions of the topic if none is provided) *)
  -> ('a -> (int*int64) -> string -> 'a)  (* function used to update an accumulator value of type 'a with an offset and a message *)
  -> (int*int64) list                     (* first offset to consume for each partition *)
  -> 'a                                   (* seed accumulator value *)
  -> 'a                                   (* final accumulated value *)                      
