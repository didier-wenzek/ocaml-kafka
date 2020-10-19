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

static void ocaml_kafka_async_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) {
  CAMLparam0();
  CAMLlocal1(caml_cb);

  caml_cb = ((ocaml_kafka_opaque*)opaque)->caml_callback;

  // do the default thing, but also call our OCaml code
  switch (err)
  {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
      rd_kafka_assign(rk, partitions);
      caml_callback(caml_cb, Val_unit);
      break;

    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
      rd_kafka_commit(rk, partitions, 0);
      rd_kafka_assign(rk, NULL);
      caml_callback(caml_cb, Val_unit);
      break;

    default:
      break;
  }

  CAMLreturn0;
}

CAMLprim value ocaml_kafka_async_new_consumer(value caml_rebalance_callback, value caml_consumer_options) {
  CAMLparam2(caml_rebalance_callback, caml_consumer_options);
  CAMLlocal1(result);

  char error_msg[160];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_res_t conf_err = configure_handler(conf, caml_consumer_options, error_msg, sizeof(error_msg));
  if (conf_err) {
    rd_kafka_conf_destroy(conf);
    result = ERROR(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka consumer (%s)", error_msg);
    CAMLreturn(result);
  }

  ocaml_kafka_opaque* opaque = NULL;
  if (Is_block(caml_rebalance_callback)) {
    opaque = ocaml_kafka_opaque_create(caml_rebalance_callback);
    rd_kafka_conf_set_opaque(conf, (void*) opaque);
    rd_kafka_conf_set_rebalance_cb(conf, ocaml_kafka_async_rebalance_cb);
  }

  rd_kafka_t *handler = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error_msg, sizeof(error_msg));
  if (handler == NULL) {
    ocaml_kafka_opaque_destroy(opaque);
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
  rd_kafka_topic_t *topic;
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
  const char* topic_name = rd_kafka_topic_name(topic);

  caml_kafka_topic = alloc_caml_handler(topic);

  if (!message->err) {
    caml_msg_payload = caml_alloc_string(message->len);
    memcpy(String_val(caml_msg_payload), message->payload, message->len);

    caml_msg_offset = caml_copy_int64(message->offset) ;

    if (message->key) {
      caml_key_payload = caml_alloc_string(message->key_len);
      memcpy(String_val(caml_key_payload), message->key, message->key_len);

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

CAMLprim value ocaml_kafka_async_produce(value caml_kafka_topic, value caml_kafka_partition, value caml_opt_key, value caml_msgid, value caml_msg) {
  CAMLparam5(caml_kafka_topic, caml_kafka_partition, caml_opt_key, caml_msgid, caml_msg);
  CAMLlocal2(caml_key, result);

  rd_kafka_topic_t *topic = handler_val(caml_kafka_topic);
  int32_t partition = RD_KAFKA_PARTITION_UA;
  if (Is_block(caml_kafka_partition) && Tag_val(caml_kafka_partition) == 0) {
    partition = Int_val(Field(caml_kafka_partition, 0));
  }

  void* payload = String_val(caml_msg);
  size_t len = caml_string_length(caml_msg);

  void* key = NULL;
  size_t key_len = 0;
  if (Is_block(caml_opt_key)) {
    caml_key = Field(caml_opt_key, 0);
    key = String_val(caml_key);
    key_len = caml_string_length(caml_key);
  }

  long msg_id = Long_val(caml_msgid);

  int err = rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY, payload, len, key, key_len, (void *)msg_id);

  if (err) {
    rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
    result = ERROR(rd_errno, "Failed to produce message (%s)", rd_kafka_err2str(rd_errno));
    CAMLreturn(result);
  }

  result = OK(Val_unit);
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
