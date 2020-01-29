let dummy _switch () = Lwt.return ()

let () =
  Alcotest.run "Lwt Unittests" [
    "all", [
      Alcotest_lwt.test_case "dummy" `Quick dummy
    ]
  ]
