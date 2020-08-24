open Kafka

external consume_job : topic -> partition -> int -> message Lwt_unix.job = "ocaml_kafka_consume_job"
let consume ?(timeout_ms = 1000) topic partition =
  Lwt_unix.run_job (consume_job topic partition timeout_ms)

external consume_queue_job : queue -> int -> message Lwt_unix.job = "ocaml_kafka_consume_queue_job"
let consume_queue ?(timeout_ms = 1000) queue =
  Lwt_unix.run_job (consume_queue_job queue timeout_ms)

external consume_batch_job : topic -> partition -> int -> int -> message list Lwt_unix.job = "ocaml_kafka_consume_batch_job"
let consume_batch ?(timeout_ms = 1000) ?(msg_count = 1024) topic partition =
  Lwt_unix.run_job (consume_batch_job topic partition timeout_ms msg_count)

external consume_batch_queue_job : queue -> int -> int -> message list Lwt_unix.job = "ocaml_kafka_consume_batch_queue_job"
let consume_batch_queue ?(timeout_ms = 1000) ?(msg_count = 1024) queue =
  Lwt_unix.run_job (consume_batch_queue_job queue timeout_ms msg_count)

let pending_msg = Hashtbl.create (8*1024)

let next_msg_id =
  let n = ref 1 in
  let get_next () =
    let id = !n in n := id + 1 ; id
  in get_next 

let produce topic ?partition ?key msg =
  let msg_id = next_msg_id () in
  let waiter, wakener = Lwt.wait () in

  Hashtbl.add pending_msg msg_id wakener;
  Kafka.produce topic ?partition ?key ~msg_id msg;
  waiter

let delivery_callback msg_id error =
  try
    let wakener = Hashtbl.find pending_msg msg_id in
    Hashtbl.remove pending_msg msg_id;
    match error with
    | None -> Lwt.wakeup wakener ()
    | Some error -> Lwt.wakeup_exn wakener (Kafka.Error (error,"Failed to produce message"))
  with Not_found -> ()

let poll_delivery period_ms producer =
  let timeout_s = (float_of_int period_ms) /. 1000.0 in
  let rec loop () = Lwt.(
    Lwt_unix.sleep timeout_s
    >>= fun () ->
    Kafka.poll_events ~timeout_ms:0 producer
    |> fun _ ->
    loop ()
  )
  in loop

let set_option options option value =
  let options = List.remove_assoc option options in
  (option,value)::options

let new_producer ?(delivery_check_period_ms=100) options =
  let options = set_option options "delivery.report.only.error" "false" in
  let producer = Kafka.new_producer ~delivery_callback options in
  Lwt.async (poll_delivery delivery_check_period_ms producer);
  producer

let wait_delivery ?(timeout_ms = 100) ?(max_outq_len = 0) producer =
  let timeout_s = (float_of_int timeout_ms) /. 1000.0 in
  let rec loop () = Lwt.(
    if Kafka.outq_len producer > max_outq_len
    then Lwt_unix.sleep timeout_s >>= loop
    else return_unit
  )
  in loop ()
