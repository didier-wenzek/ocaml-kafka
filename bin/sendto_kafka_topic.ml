open Cmdliner
open Lwt

let info =
  let doc = "Send to kafka all messages read from standard input" in
  let man =
    [
      `S "DESCRIPTION";
      `P
        "$(tname) reads standard input and sends each line to the TOPIC kafka \
         topic.";
      `P
        "Scatter messages over all partitions, unless a specific PARTITION is \
         given.";
    ]
  in
  Cmd.info "sendto_kafka_topic" ~doc ~man

let sendto_topic brokers topic_name partition =
  let producer = Kafka_lwt.new_producer [ ("metadata.broker.list", brokers) ] in
  let topic = Kafka.new_topic producer topic_name [] in
  let send msg () = Kafka_lwt.produce topic ?partition msg in
  let report_error msg error =
    Lwt_io.fprintf Lwt_io.stderr "%s:%s\n%!" (Printexc.to_string error) msg
  in
  let rec loop () =
    try_bind
      (fun () -> Lwt_io.read_line Lwt_io.stdin)
      (fun msg ->
        async (fun () -> catch (send msg) (report_error msg));
        loop ())
      (function End_of_file -> return_unit | exp -> Lwt.fail exp)
  in
  let term () =
    Kafka_lwt.wait_delivery producer >>= fun () ->
    Kafka.destroy_topic topic;
    Kafka.destroy_handler producer;
    return ()
  in
  Lwt_main.run (Lwt.finalize loop term)

let brokers =
  let doc = "Kafka broker hosts (comma sepated list of 'host:port')" in
  Arg.(
    value & opt string "localhost"
    & info [ "b"; "brokers" ] ~docv:"BROKERS" ~doc)

let topic =
  let doc = "The topic to feed." in
  Arg.(required & pos 0 (some string) None & info [] ~docv:"TOPIC" ~doc)

let partition =
  let doc = "The partition to feed (all if unassigned)." in
  Arg.(value & opt (some int) None & info [] ~docv:"PARTITION" ~doc)

let sendto_topic_t = Term.(const sendto_topic $ brokers $ topic $ partition)
let () = Cmd.v info sendto_topic_t |> Cmd.eval |> exit
