(** A sink is defined as a function used to open an output stream.
    This function returns a pair of functions (push,close)
    aimed to push data into the stream
    and to finally close the stream.
*)
type ('a,'b) sink = unit -> ('a -> unit) * (unit -> 'b) 

type 'a iterable = ('a -> unit) -> unit
val stream_to: ('a,'b) sink -> 'a iterable -> 'b

type 'a push_error_handler = ('a -> unit) -> 'a -> exn -> unit
val retry_on_error: 'a push_error_handler
val raise_on_error: 'a push_error_handler

val partition_sink:
     ?producer_props:(string*string) list
  -> ?topic_props:(string*string) list
  -> ?delivery_error_handler:string push_error_handler
  -> string -> int                                      (* topic name and partition to stream into *)
  -> (string,unit) sink  

