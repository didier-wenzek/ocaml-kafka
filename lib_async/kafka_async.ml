open Core
open Async

let pending_table = Int.Table.create ~size:(8 * 1024)

type 'a response = ('a, Kafka.error * string) result

type producer = {
  handler : Kafka.handler;
  pending_msg : unit Ivar.t Int.Table.t;
  stop_poll : unit Ivar.t;
}

type consumer = {
  handler : Kafka.handler;
  start_poll : unit Ivar.t;
  stop_poll : unit Ivar.t;
  subscriptions : Kafka.message Pipe.Writer.t String.Table.t;
}

let next_msg_id =
  let n = ref 1 in
  fun () ->
    let id = !n in
    n := id + 1;
    id

let poll_interval = Time.Span.of_ms 50.

external poll' : Kafka.handler -> int = "ocaml_kafka_async_poll"

let produce (t : producer) topic ?partition ?key msg =
  let msg_id = next_msg_id () in
  let ivar = Ivar.create () in
  Int.Table.add_exn t.pending_msg ~key:msg_id ~data:ivar;
  Kafka.produce topic ?partition ?key ~msg_id msg ;
  Ivar.read ivar |> Deferred.ok

external new_producer' :
  (Kafka.msg_id -> Kafka.error option -> unit) ->
  (string * string) list ->
  Kafka.handler response = "ocaml_kafka_async_new_producer"

let handle_producer_response pending_msg msg_id _maybe_error =
  match Int.Table.find_and_remove pending_msg msg_id with
  | Some ivar -> Ivar.fill ivar ()
  | None -> ()

let new_producer xs =
  let open Result.Let_syntax in
  let pending_msg = pending_table () in
  let stop_poll = Ivar.create () in
  let%bind handler = new_producer' (handle_producer_response pending_msg) xs in
  every ~stop:(Ivar.read stop_poll) poll_interval (fun () ->
      ignore (poll' handler));
  return { handler; pending_msg; stop_poll }

external new_consumer' : (string * string) list -> Kafka.handler response
  = "ocaml_kafka_async_new_consumer"

external consumer_poll' : Kafka.handler -> Kafka.message option response
  = "ocaml_kafka_async_consumer_poll"

let handle_incoming_message subscriptions = function
  | None | Some (Kafka.PartitionEnd _) -> ()
  | Some (Kafka.Message (topic, _, _, _, _) as msg) -> (
      let topic_name = Kafka.topic_name topic in
      match String.Table.find subscriptions topic_name with
      | None -> ()
      | Some writer -> Pipe.write_without_pushback writer msg)

let new_consumer xs =
  let open Result.Let_syntax in
  let subscriptions = String.Table.create ~size:(8 * 1024) () in
  let stop_poll = Ivar.create () in
  let start_poll = Ivar.create () in
  let%bind handler = new_consumer' xs in
  every ~start:(Ivar.read start_poll) ~stop:(Ivar.read stop_poll) poll_interval
    (fun () ->
      match consumer_poll' handler with
      | Error _ ->
          Log.Global.error "Issue with polling";
          ()
      | Ok success -> handle_incoming_message subscriptions success);
  return { handler; subscriptions; start_poll; stop_poll }

external subscribe' : Kafka.handler -> topics:string list -> unit response
  = "ocaml_kafka_async_subscribe"

let consume consumer ~topic =
  let open Result.Let_syntax in
  match String.Table.mem consumer.subscriptions topic with
  | true -> Error (Kafka.FAIL, "Already subscribed to this topic")
  | false ->
      Ivar.fill_if_empty consumer.start_poll ();
      let subscribe_error = ref None in
      let reader =
        Pipe.create_reader ~close_on_exception:false (fun writer ->
            String.Table.add_exn consumer.subscriptions ~key:topic ~data:writer;
            let topics = String.Table.keys consumer.subscriptions in
            match subscribe' consumer.handler ~topics with
            | Ok () -> Ivar.read consumer.stop_poll
            | Error e ->
                subscribe_error := Some e;
                Deferred.return ())
      in
      don't_wait_for
        (let open Deferred.Let_syntax in
        let%map () = Pipe.closed reader in
        String.Table.remove consumer.subscriptions topic;
        let remaining_subs = String.Table.keys consumer.subscriptions in
        ignore @@ subscribe' consumer.handler ~topics:remaining_subs);
      match Pipe.is_closed reader with
      | false -> return reader
      | true  ->
          match !subscribe_error with
          | None -> Error (Kafka.FAIL, "Programmer error, subscribe_error unset")
          | Some e -> Error e

let new_topic (producer : producer) name opts =
  match Kafka.new_topic producer.handler name opts with
  | v -> Ok v
  | exception Kafka.Error (e, msg) -> Error (e, msg)

let destroy_consumer consumer =
  Ivar.fill consumer.stop_poll ();
  Kafka.destroy_handler consumer.handler

let destroy_producer (producer : producer) =
  Ivar.fill producer.stop_poll ();
  Kafka.destroy_handler producer.handler
