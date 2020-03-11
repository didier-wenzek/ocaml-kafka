#ifndef OCAML_KAFKA_H
#define OCAML_KAFKA_H

/* Configuration results (of type rd_kafka_conf_res_t) are merge with errors (of type rd_kafka_resp_err_t).
   So we have a single Error type ocaml side. */
#define RD_KAFKA_CONF_RES(kafka_conf_res) (RD_KAFKA_RESP_ERR__END + kafka_conf_res)

#define handler_val(v) *((void **) &Field(v, 0))

value caml_error_value(rd_kafka_resp_err_t);
rd_kafka_conf_res_t configure_handler(rd_kafka_conf_t *, value, char*, size_t);
void ocaml_kafka_delivery_callback(rd_kafka_t*, void*, size_t, rd_kafka_resp_err_t, void*, void*);
value alloc_caml_handler(void*);

typedef struct {
  value caml_callback;
} ocaml_kafka_opaque;

ocaml_kafka_opaque* ocaml_kafka_opaque_create(value);
void ocaml_kafka_opaque_destroy(ocaml_kafka_opaque*);

#endif /* OCAML_KAFKA_H */
