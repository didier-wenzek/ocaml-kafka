module C = Configurator.V1

(* the -I and -L flags are required for freebsd, harmless elsewhere *)
let default : C.Pkg_config.package_conf =
  {
    libs = [ "-L/usr/local/lib"; "-lrdkafka"; "-lpthread"; "-lz" ];
    cflags = [ "-I/usr/local/include" ];
  }

let () =
  C.main ~name:"kafka" (fun c ->
      let default = default in
      let conf =
        match C.Pkg_config.get c with
        | None -> default
        | Some pc -> (
            match C.Pkg_config.query pc ~package:"rdkafka" with
            | Some s -> s
            | None -> default)
      in
      C.Flags.write_sexp "c_library_flags.sexp" conf.libs;
      C.Flags.write_sexp "c_flags.sexp" conf.cflags)
