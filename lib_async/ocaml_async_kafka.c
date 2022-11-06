#include <stdarg.h>
#include <string.h>

#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/callback.h>
#include <caml/mlvalues.h>

#include <librdkafka/rdkafka.h>

#include "ocaml_kafka.h"

static CAMLprim value ERROR(rd_kafka_resp_err_t rd_errno, const char *error, ...) {
  CAMLparam0();
  CAMLlocal3(error_tuple, error_string, result_record);

  static char error_msg[160];
  va_list ap;
  va_start(ap, error);
  vsnprintf(error_msg, sizeof(error_msg), error, ap);
  va_end(ap);

  error_string = caml_copy_string(error_msg);
  error_tuple = caml_alloc_tuple(2);
  Store_field(error_tuple, 0, caml_error_value(rd_errno));
  Store_field(error_tuple, 1, error_string);

  /* second non-empty constructor in result = Ok | Error */
  int error_tag = 1;
  result_record = caml_alloc(1, error_tag);
  Store_field(result_record, 0, error_tuple);

  CAMLreturn(result_record);
}

static CAMLprim value OK(value payload) {
  CAMLparam1(payload);
  CAMLlocal1(result_record);

  int success_tag = 0;
  result_record = caml_alloc(1, success_tag);
  Store_field(result_record, 0, payload);

  CAMLreturn(result_record);
}

CAMLprim value ocaml_kafka_async_new_producer(value caml_delivery_callback, value caml_producer_options) {
  CAMLparam2(caml_delivery_callback, caml_producer_options);
  CAMLlocal2(caml_callback, result);

  ocaml_kafka_opaque* opaque = NULL;
  char error_msg[160];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_res_t conf_err = configure_handler(conf, caml_producer_options, error_msg, sizeof(error_msg));
  if (conf_err) {
    rd_kafka_conf_destroy(conf);
    result = ERROR(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka producer (%s)", error_msg);
    CAMLreturn(result);
  }

  opaque = ocaml_kafka_opaque_create(caml_delivery_callback);
  rd_kafka_conf_set_opaque(conf, (void*) opaque);
  rd_kafka_conf_set_dr_cb(conf, ocaml_kafka_delivery_callback);

  rd_kafka_t *handler = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error_msg, sizeof(error_msg));
  if (handler == NULL) {
    ocaml_kafka_opaque_destroy(opaque);
    rd_kafka_conf_destroy(conf);
    result = ERROR(RD_KAFKA_RESP_ERR__FAIL, "Failed to create new kafka producer (%s)", error_msg);
    CAMLreturn(result);
  }

  value caml_handler = alloc_caml_handler(handler);
  result = OK(caml_handler);
  CAMLreturn(result);
}

CAMLprim value ocaml_kafka_async_new_consumer(value caml_consumer_options) {
  CAMLparam1(caml_consumer_options);
  CAMLlocal1(result);

  char error_msg[160];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_res_t conf_err = configure_handler(conf, caml_consumer_options, error_msg, sizeof(error_msg));
  if (conf_err) {
    rd_kafka_conf_destroy(conf);
    result = ERROR(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka consumer (%s)", error_msg);
    CAMLreturn(result);
  }

  rd_kafka_t *handler = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error_msg, sizeof(error_msg));
  if (handler == NULL) {
    rd_kafka_conf_destroy(conf);
    result = ERROR(RD_KAFKA_RESP_ERR__FAIL, "Failed to create new kafka consumer (%s)", error_msg);
    CAMLreturn(result);
  }

  rd_kafka_poll_set_consumer(handler);

  value caml_handler = alloc_caml_handler(handler);
  result = OK(caml_handler);
  CAMLreturn(result);
}

CAMLprim value ocaml_kafka_async_subscribe(value caml_kafka_handler, value caml_topic_list) {
  CAMLparam2(caml_kafka_handler, caml_topic_list);
  CAMLlocal3(result, hd, tl);

  rd_kafka_topic_partition_list_t *subscription;
  rd_kafka_resp_err_t err;
  rd_kafka_t* rk = handler_val(caml_kafka_handler);
  int topic_count = 0;
  const char* topic_name;

  if (Int_val(caml_topic_list) == 0) {
    /* subscribe to no topics aka unsubscribe from all */
    rd_kafka_unsubscribe(rk);
  } else {
    hd = caml_topic_list;
    while (Int_val(hd) != 0) {
      tl = Field(hd, 1);
      hd = tl;
      topic_count++;
    }
    subscription = rd_kafka_topic_partition_list_new(topic_count);

    hd = caml_topic_list;
    while (Int_val(hd) != 0) {
      topic_name = String_val(Field(hd, 0));
      /* we have a head, so there is a tail */
      tl = Field(hd, 1);
      hd = tl;

      rd_kafka_topic_partition_list_add(subscription, topic_name, RD_KAFKA_PARTITION_UA);
    }

    err = rd_kafka_subscribe(rk, subscription);

    if (err) {
      result = ERROR(err, "Failed to subscribe (%s)", rd_kafka_err2str(err));
      rd_kafka_topic_partition_list_destroy(subscription);
      CAMLreturn(result);
    }
    rd_kafka_topic_partition_list_destroy(subscription);
  }

  result = OK(Val_unit);
  CAMLreturn(result);
}

CAMLprim value ocaml_kafka_async_extract_message(rd_kafka_message_t* message) {
  CAMLparam0();
  CAMLlocal5(caml_msg, caml_msg_payload, caml_msg_offset, caml_key, caml_key_payload);
  CAMLlocal2(result, caml_kafka_topic);

  rd_kafka_topic_t* topic = message->rkt;

  caml_kafka_topic = alloc_caml_handler(topic);

  if (!message->err) {
    caml_msg_payload = caml_alloc_initialized_string(message->len, message->payload);
    caml_msg_offset = caml_copy_int64(message->offset) ;

    if (message->key) {
      caml_key_payload = caml_alloc_initialized_string(message->key_len, message->key);

      caml_key = caml_alloc_small(1, 0); // Some(key)
      Field(caml_key, 0) = caml_key_payload;
    } else {
      caml_key = Val_int(0); // None
    }

    caml_msg = caml_alloc_small(5, 0);
    Field(caml_msg, 0) = caml_kafka_topic;
    Field(caml_msg, 1) = Val_int(message->partition);
    Field(caml_msg, 2) = caml_msg_offset;
    Field(caml_msg, 3) = caml_msg_payload;
    Field(caml_msg, 4) = caml_key;
    result = caml_msg;
  } else {
    caml_msg_offset = caml_copy_int64(message->offset) ;
    caml_msg = caml_alloc_small(3, 1);
    Field(caml_msg, 0) = caml_kafka_topic;
    Field(caml_msg, 1) = Val_int(message->partition);
    Field(caml_msg, 2) = caml_msg_offset;
    result = caml_msg;
  }

  CAMLreturn(result);
}

CAMLprim value ocaml_kafka_async_consumer_poll(value caml_kafka_handler) {
  CAMLparam1(caml_kafka_handler);
  CAMLlocal2(result, option);

  rd_kafka_message_t *rkm;

  rd_kafka_t* rk = handler_val(caml_kafka_handler);
  rkm = rd_kafka_consumer_poll(rk, 0);

  if (!rkm) {
    option = Val_int(0); // None
    result = OK(option);
    CAMLreturn(result);
  }

  if (rkm->err && rkm->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    if (rkm->payload) {
      result = ERROR(rkm->err, "Consumed message with error (%s)", (const char *)rkm->payload);
    } else {
      result = ERROR(rkm->err, "Consumed message with error (%s)", rd_kafka_err2str(rkm->err));
    }
  } else {
    option = caml_alloc_small(1, 0);
    Field(option, 0) = ocaml_kafka_async_extract_message(rkm);
    result = OK(option);
  }

  CAMLreturn(result);
}

CAMLprim value ocaml_kafka_async_poll(value caml_kafka_handler) {
  CAMLparam1(caml_kafka_handler);
  CAMLlocal1(caml_events);

  rd_kafka_t *rk = handler_val(caml_kafka_handler);
  int timeout_ms = 0;
  int events = rd_kafka_poll(rk, timeout_ms);
  caml_events = Val_int(events);

  CAMLreturn(caml_events);
}
