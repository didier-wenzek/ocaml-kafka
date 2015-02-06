(** A sink is defined as a function used to open an output stream.
    This function returns a pair of functions (push,close)
    aimed to push data into the stream
    and to finally close the stream.
*)
type ('a,'b) sink = unit -> ('a -> unit) * (unit -> 'b) 

type 'a iterable = ('a -> unit) -> unit
val stream_to: ('a,'b) sink -> 'a iterable -> 'b

val partition_sink:
     ?producer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> string -> int                            (* topic name and partition to stream into *)
  -> (string,unit) sink  

