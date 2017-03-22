open Ocamlbuild_plugin

let _ = dispatch begin function
    | After_rules ->
        ocaml_lib "okafka";

        flag ["ocamlmklib"; "c"; "use_rdkafka"] (S[A"-lrdkafka"; A"-lpthread"; A"-lz"]);
        flag ["link"; "ocaml"; "use_rdkafka"] (S[A"-cclib"; A"-lrdkafka"; A"-cclib"; A"-lpthread"; A"-cclib"; A"-lz"]);

        flag ["link";"library";"ocaml";"byte";"use_kafka"] & S[A"-dllib";A"-locamlkafka";A"-cclib";A"-L.";A"-cclib";A"-locamlkafka"];
        flag ["link";"library";"ocaml";"native";"use_kafka"] & S[A"-cclib";A"-L.";A"-cclib";A"-locamlkafka"];
        flag ["link";"ocaml";"link_rdkafka"] (A"libocamlkafka.a");

        dep ["link";"ocaml";"use_rdkafka"] ["libocamlkafka.a"];
        dep ["link";"ocaml";"link_rdkafka"] ["libocamlkafka.a"];

    | _ -> ()
end
