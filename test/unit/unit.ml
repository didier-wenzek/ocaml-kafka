let dummy () = ()

let () =
  Alcotest.run "Unittests" [
    "all", [
      Alcotest.test_case "dummy" `Quick dummy

    ]
  ]
