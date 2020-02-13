#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <caml/alloc.h>
#include <caml/version.h>

#include <string.h>
#include <stdarg.h>
#include <librdkafka/rdkafka.h>

#include "ocaml_kafka.h"

/* Default consumer timeout. */
#define DEFAULT_TIMEOUT_MS 1000

/* Default batch size. */
#define DEFAULT_MSG_COUNT 1024

/* Must be synchronized with Kafka.Error. */
static int const ERROR_CODES[] = {
    // not an error RD_KAFKA_RESP_ERR__BEGIN
    RD_KAFKA_RESP_ERR__BAD_MSG,
    RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
    RD_KAFKA_RESP_ERR__DESTROY,
    RD_KAFKA_RESP_ERR__FAIL,
    RD_KAFKA_RESP_ERR__TRANSPORT,
    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
    RD_KAFKA_RESP_ERR__RESOLVE,
    RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
    RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
    RD_KAFKA_RESP_ERR__FS,
    RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC,
    RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
    RD_KAFKA_RESP_ERR__INVALID_ARG,
    RD_KAFKA_RESP_ERR__TIMED_OUT,
    RD_KAFKA_RESP_ERR__QUEUE_FULL,
    RD_KAFKA_RESP_ERR__ISR_INSUFF,
    // not an error RD_KAFKA_RESP_ERR__END
    
    RD_KAFKA_RESP_ERR_UNKNOWN,
    RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE,
    RD_KAFKA_RESP_ERR_INVALID_MSG,
    RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
    RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE,
    RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE,
    RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION,
    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT,
    RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE,
    RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE,
    RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
    RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH,
    RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE,

    RD_KAFKA_CONF_RES(RD_KAFKA_CONF_UNKNOWN),
    RD_KAFKA_CONF_RES(RD_KAFKA_CONF_INVALID)
};

static int const EUNKNOWN = (sizeof ERROR_CODES) / (sizeof ERROR_CODES[0]);

value caml_error_value(rd_kafka_resp_err_t rd_errno)
{
  CAMLparam0();
  int i;
  for (i = 0; i < EUNKNOWN; i++) {
    if (rd_errno == ERROR_CODES[i]) {
      CAMLreturn(Val_int(i));
    }
  }

  CAMLreturn(Val_int(RD_KAFKA_RESP_ERR_UNKNOWN));
}

#define RAISE ocaml_kafka_raise
void ocaml_kafka_raise(rd_kafka_resp_err_t rd_errno, const char *error, ...)
{
  CAMLparam0();
  CAMLlocalN(error_parameters, 2);
  static const value *exception_handler = NULL;
  static char error_msg[160];
  va_list ap;
  va_start(ap, error);
  vsnprintf(error_msg, sizeof(error_msg), error, ap);
  va_end(ap);

  if (exception_handler == NULL) {
    exception_handler = caml_named_value("kafka.error");
    if (exception_handler == NULL) {
      caml_failwith(error_msg);
    }
  }

  error_parameters[0] = caml_error_value(rd_errno);
  error_parameters[1] = caml_copy_string(error_msg);
  caml_raise_with_args(*exception_handler, 2, error_parameters);
  CAMLnoreturn;
}

value alloc_caml_handler(void* hdl)
{
  CAMLparam0();
  CAMLlocal1(caml_handler);

  caml_handler = alloc_small(1, Abstract_tag);
  handler_val(caml_handler) = hdl;

  CAMLreturn(caml_handler);
}

inline static void free_caml_handler(value caml_handler)
{
  handler_val(caml_handler) = NULL;
}

inline static void* get_handler(value caml_handler)
{
  void* hdl = handler_val(caml_handler);
  if (! hdl) {
    RAISE(RD_KAFKA_RESP_ERR__INVALID_ARG, "Kafka handler has been released");
  }

  return hdl;
}

rd_kafka_conf_res_t configure_handler(rd_kafka_conf_t *conf, value caml_options, char *errstr, size_t errstr_size)
{
  CAMLparam0();
  CAMLlocal3(caml_option_pair, caml_option_name, caml_option_value);

  rd_kafka_conf_res_t conf_err;
  while (caml_options != Val_emptylist) {
    caml_option_pair = Field(caml_options, 0);
    caml_options = Field(caml_options, 1);
    caml_option_name = Field(caml_option_pair,0);
    caml_option_value = Field(caml_option_pair,1);

    conf_err = rd_kafka_conf_set(conf, String_val(caml_option_name), String_val(caml_option_value), errstr, errstr_size);
    if (conf_err) return conf_err;
  }
  CAMLreturn(RD_KAFKA_CONF_OK);
};

static
rd_kafka_conf_res_t configure_topic(rd_kafka_topic_conf_t *conf, value caml_options, char *errstr, size_t errstr_size)
{
  CAMLparam0();
  CAMLlocal3(caml_option_pair, caml_option_name, caml_option_value);

  rd_kafka_conf_res_t conf_err;
  while (caml_options != Val_emptylist) {
    caml_option_pair = Field(caml_options, 0);
    caml_options = Field(caml_options, 1);
    caml_option_name = Field(caml_option_pair,0);
    caml_option_value = Field(caml_option_pair,1);

    conf_err = rd_kafka_topic_conf_set(conf, String_val(caml_option_name), String_val(caml_option_value), errstr, errstr_size);
    if (conf_err) return conf_err;
  }
  CAMLreturn(RD_KAFKA_CONF_OK);
};

extern CAMLprim
value ocaml_kafka_new_consumer(value caml_consumer_options)
{
  CAMLparam1(caml_consumer_options);

  char error_msg[160];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_res_t conf_err = configure_handler(conf, caml_consumer_options, error_msg, sizeof(error_msg));
  if (conf_err) {
     rd_kafka_conf_destroy(conf);
     RAISE(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka consumer (%s)", error_msg);
  }

  rd_kafka_t *handler = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error_msg, sizeof(error_msg));
  if (handler == NULL) {
     rd_kafka_conf_destroy(conf);
     RAISE(RD_KAFKA_RESP_ERR__FAIL, "Failed to create new kafka consumer (%s)", error_msg);
  }

  value caml_handler = alloc_caml_handler(handler);
  CAMLreturn(caml_handler);
}

typedef struct {
  value caml_callback;
} ocaml_kafka_opaque;

ocaml_kafka_opaque* ocaml_kafka_opaque_create(value caml_callback) {
  CAMLparam1(caml_callback);

  ocaml_kafka_opaque* opaque = malloc(sizeof (ocaml_kafka_opaque));
  if (opaque) {
#if OCAML_VERSION >= 40900
    caml_register_generational_global_root(&opaque->caml_callback);
    caml_modify_generational_global_root(&opaque->caml_callback, caml_callback);
#else
    caml_register_global_root(&opaque->caml_callback);
    opaque->caml_callback = caml_callback;
#endif /* OCAML_VERSION >= 40900 */
  }
  CAMLreturnT(ocaml_kafka_opaque*, opaque);
}

void ocaml_kafka_opaque_destroy(ocaml_kafka_opaque* opaque) {
  if (opaque) {
#if OCAML_VERSION >= 40900
    caml_remove_generational_global_root(&opaque->caml_callback);
#else
    caml_remove_global_root(&opaque->caml_callback);
#endif /* OCAML_VERSION >= 40900 */
    free(opaque);
  }
}

void ocaml_kafka_delivery_callback(rd_kafka_t *producer, void *payload, size_t len, rd_kafka_resp_err_t err, void *opaque, void *msg_opaque)
{
  CAMLparam0();
  CAMLlocal3(caml_callback, caml_msg_id, caml_error);

  caml_callback = ((ocaml_kafka_opaque*)opaque)->caml_callback;

  if (msg_opaque) {
    long msg_id = (long) msg_opaque;    // has been set by ocaml_kafka_produce
    caml_msg_id = Val_long(msg_id);
  } else {
    caml_msg_id = Val_long(0);          // None
  }

  if (! err) {
    caml_error = Val_int(0);            // None
  } else {
    caml_error = caml_alloc_small(1,0); // Some(error)
    Field(caml_error, 0) = caml_error_value(err);
  }

  caml_callback2(caml_callback, caml_msg_id, caml_error);
  CAMLreturn0;
}

extern CAMLprim
value ocaml_kafka_new_producer(value caml_delivery_callback, value caml_producer_options)
{
  CAMLparam2(caml_delivery_callback, caml_producer_options);
  CAMLlocal1(caml_callback);

  char error_msg[160];
  ocaml_kafka_opaque* opaque = NULL;

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_res_t conf_err = configure_handler(conf, caml_producer_options, error_msg, sizeof(error_msg));
  if (conf_err) {
     rd_kafka_conf_destroy(conf);
     RAISE(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka producer (%s)", error_msg);
  }

  if (Is_block(caml_delivery_callback)) {
     caml_callback = Field(caml_delivery_callback, 0);
     opaque = ocaml_kafka_opaque_create(caml_callback);
     rd_kafka_conf_set_opaque(conf, (void*) opaque);
     rd_kafka_conf_set_dr_cb(conf, ocaml_kafka_delivery_callback);
  } 

  rd_kafka_t *handler = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error_msg, sizeof(error_msg));
  if (handler == NULL) {
     ocaml_kafka_opaque_destroy(opaque);
     rd_kafka_conf_destroy(conf);
     RAISE(RD_KAFKA_RESP_ERR__FAIL, "Failed to create new kafka producer (%s)", error_msg);
  }

  value caml_handler = alloc_caml_handler(handler);
  CAMLreturn(caml_handler);
}

extern CAMLprim
value ocaml_kafka_destroy_handler(value caml_kafka_handler)
{
  CAMLparam1(caml_kafka_handler);

  rd_kafka_t *handler = handler_val(caml_kafka_handler);
  if (handler) {
    ocaml_kafka_opaque* opaque = rd_kafka_opaque(handler);
    ocaml_kafka_opaque_destroy(opaque);
    free_caml_handler(caml_kafka_handler);
    rd_kafka_destroy(handler);
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_handler_name(value caml_kafka_handler)
{
  CAMLparam1(caml_kafka_handler);
  CAMLlocal1(caml_name);

  rd_kafka_t *handler = get_handler(caml_kafka_handler);
  const char* name = rd_kafka_name(handler);
  
  size_t len = strlen(name);
  caml_name = caml_alloc_string(len);
  memcpy(String_val(caml_name), name, len);

  CAMLreturn(caml_name);
}

static
int32_t ocaml_kafka_partitioner_callback(const rd_kafka_topic_t *topic, const void *key, size_t keylen, int32_t partition_cnt, void *opaque, void *msg_opaque)
{
  CAMLparam0();
  CAMLlocal4(caml_callback, caml_key, caml_partition_cnt, caml_partition);

  caml_callback = ((ocaml_kafka_opaque*)opaque)->caml_callback;
  caml_partition_cnt = Val_int(partition_cnt);
  caml_key = caml_alloc_string(keylen);
  memcpy(String_val(caml_key), key, keylen);

  caml_partition = caml_callback2(caml_callback, caml_partition_cnt, caml_key);
  CAMLreturn(Int_val(caml_partition));
}

extern CAMLprim
value ocaml_kafka_new_topic(value caml_partitioner_callback, value caml_kafka_handler, value caml_topic_name, value caml_topic_options)
{
  CAMLparam4(caml_partitioner_callback, caml_kafka_handler, caml_topic_name, caml_topic_options);
  CAMLlocal2(caml_callback, caml_kafka_topic_handler);

  ocaml_kafka_opaque* opaque = NULL;
  rd_kafka_t *handler = get_handler(caml_kafka_handler);
  const char* name = String_val(caml_topic_name);

  rd_kafka_topic_conf_t *conf = rd_kafka_topic_conf_new();
  if (Is_block(caml_partitioner_callback)) {
     caml_callback = Field(caml_partitioner_callback, 0);
     opaque = ocaml_kafka_opaque_create(caml_callback);
     rd_kafka_topic_conf_set_opaque(conf, (void*) opaque);
     rd_kafka_topic_conf_set_partitioner_cb(conf, ocaml_kafka_partitioner_callback);
  } 

  char error_msg[160];
  rd_kafka_conf_res_t conf_err = configure_topic(conf, caml_topic_options, error_msg, sizeof(error_msg));
  if (conf_err) {
     rd_kafka_topic_conf_destroy(conf);
     ocaml_kafka_opaque_destroy(opaque);
     RAISE(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka topic (%s)", error_msg);
  }

  rd_kafka_topic_t* topic = rd_kafka_topic_new(handler, name, conf);
  if (!topic) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
     ocaml_kafka_opaque_destroy(opaque);
     RAISE(rd_errno, "Failed to create new kafka topic (%s)", rd_kafka_err2str(rd_errno));
  }

  caml_kafka_topic_handler = alloc_caml_handler(topic);
  CAMLreturn(caml_kafka_topic_handler);
}

extern CAMLprim
value ocaml_kafka_destroy_topic(value caml_kafka_topic)
{
  CAMLparam1(caml_kafka_topic);

  rd_kafka_topic_t *topic = handler_val(caml_kafka_topic);
  if (topic) {
    ocaml_kafka_opaque* opaque = rd_kafka_topic_opaque(topic);
    ocaml_kafka_opaque_destroy(opaque);
    free_caml_handler(caml_kafka_topic);
    rd_kafka_topic_destroy(topic);
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_topic_name(value caml_kafka_topic)
{
  CAMLparam1(caml_kafka_topic);

  rd_kafka_topic_t* rkt = get_handler(caml_kafka_topic);
  CAMLreturn(caml_copy_string(rd_kafka_topic_name(rkt)));
}

extern CAMLprim
value ocaml_kafka_consume_start(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_offset)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_kafka_offset);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32_t partition = Int_val(caml_kafka_partition);
  int64_t offset = Int64_val(caml_kafka_offset);
  int err = rd_kafka_consume_start(topic, partition, offset);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
     RAISE(rd_errno, "Failed to start consuming messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_consume_stop(value caml_kafka_topic, value caml_kafka_partition)
{
  CAMLparam2(caml_kafka_topic,caml_kafka_partition);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32_t partition = Int_val(caml_kafka_partition);
  int err = rd_kafka_consume_stop(topic, partition);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
     RAISE(rd_errno, "Failed to stop consuming messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern
value ocaml_kafka_extract_topic_message(value caml_kafka_topic, rd_kafka_message_t* message)
{
  CAMLparam1(caml_kafka_topic);
  CAMLlocal5(caml_msg, caml_msg_payload, caml_msg_offset, caml_key, caml_key_payload);

  if (!message->err) {
    caml_msg_payload = caml_alloc_string(message->len);
    memcpy(String_val(caml_msg_payload), message->payload, message->len);
    caml_msg_offset = caml_copy_int64(message->offset) ;

    if (message->key) {
      caml_key_payload = caml_alloc_string(message->key_len);
      memcpy(String_val(caml_key_payload), message->key, message->key_len);

      caml_key = caml_alloc_small(1,0); // Some(key)
      Field(caml_key, 0) = caml_key_payload; 
    } else {
      caml_key = Val_int(0); // None
    }

    caml_msg = caml_alloc_small(5, 0);
    Field( caml_msg, 0) = caml_kafka_topic;
    Field( caml_msg, 1) = Val_int(message->partition);
    Field( caml_msg, 2) = caml_msg_offset;
    Field( caml_msg, 3) = caml_msg_payload;
    Field( caml_msg, 4) = caml_key;
  }
  else if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    caml_msg_offset = caml_copy_int64(message->offset) ;
    caml_msg = caml_alloc_small(3, 1);
    Field( caml_msg, 0) = caml_kafka_topic;
    Field( caml_msg, 1) = Val_int(message->partition);
    Field( caml_msg, 2) = caml_msg_offset;
  }
  else {
    if (message->payload) {
       RAISE(message->err, "Consumed message with error (%s)", (const char *)message->payload);
    } else {
       RAISE(message->err, "Consumed message with error (%s)", rd_kafka_err2str(message->err));
    }
  }

  CAMLreturn(caml_msg);
}

extern
value ocaml_kafka_extract_topic_message_list(value caml_kafka_topic, rd_kafka_message_t** messages, size_t msg_count)
{
  CAMLparam1(caml_kafka_topic);
  CAMLlocal4(caml_msg_list, caml_new_cons, caml_last_cons, caml_msg);

  size_t i;
  caml_msg_list = Val_emptylist;
  for (i = 0; i<msg_count; ++i) {
    caml_msg = ocaml_kafka_extract_topic_message(caml_kafka_topic, messages[i]);
    caml_new_cons = caml_alloc(2,0);
    Store_field(caml_new_cons, 0, caml_msg);
    Store_field(caml_new_cons, 1, Val_emptylist);
    if (i == 0) {
       caml_msg_list = caml_new_cons;
    } else {
       Store_field(caml_last_cons, 1, caml_new_cons);
    }
    caml_last_cons = caml_new_cons;
  }

  CAMLreturn(caml_msg_list);
}

extern CAMLprim
value ocaml_kafka_consume(value caml_kafka_timeout, value caml_kafka_topic, value caml_kafka_partition)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_kafka_timeout);
  CAMLlocal1(caml_msg);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32_t partition = Int_val(caml_kafka_partition);
  int timeout = DEFAULT_TIMEOUT_MS;
  if (Is_block(caml_kafka_timeout)) {
    int t = Int_val(Field(caml_kafka_timeout, 0));
    timeout = t>=0 ? t : DEFAULT_TIMEOUT_MS;
  }

  rd_kafka_message_t* message = rd_kafka_consume(topic, partition, timeout);

  if (message) {
    caml_msg = ocaml_kafka_extract_topic_message(caml_kafka_topic, message);
    rd_kafka_message_destroy(message);
  } else {
    rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
    RAISE(rd_errno, "Failed to consume message (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(caml_msg);
}

extern CAMLprim
value ocaml_kafka_consume_batch(value caml_kafka_timeout, value caml_msg_count, value caml_kafka_topic, value caml_kafka_partition)
{
  CAMLparam4(caml_kafka_topic,caml_kafka_partition,caml_kafka_timeout, caml_msg_count);
  CAMLlocal1(caml_msg_list);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32_t partition = Int_val(caml_kafka_partition);
  int timeout = DEFAULT_TIMEOUT_MS;
  if (Is_block(caml_kafka_timeout)) {
    int t = Int_val(Field(caml_kafka_timeout, 0));
    timeout = t>=0 ? t : DEFAULT_TIMEOUT_MS;
  }
  size_t msg_count = DEFAULT_MSG_COUNT;
  if (Is_block(caml_msg_count)) {
    int n = Int_val(Field(caml_msg_count, 0));
    msg_count = n>=0 ? n : 0;
  }

  rd_kafka_message_t* messages[msg_count];

  ssize_t actual_msg_count = rd_kafka_consume_batch(topic, partition, timeout, messages, msg_count);

  if (actual_msg_count >= 0) {
    caml_msg_list = ocaml_kafka_extract_topic_message_list(caml_kafka_topic, messages, actual_msg_count);
    size_t i;
    for (i = 0; i<actual_msg_count; ++i) {
      rd_kafka_message_destroy(messages[i]);
    }
  } else {
    rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
    RAISE(rd_errno, "Failed to consume messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(caml_msg_list);
}

extern CAMLprim
value ocaml_kafka_produce(value caml_kafka_topic, value caml_kafka_partition, value caml_opt_key, value caml_msgid, value caml_msg)
{
  CAMLparam5(caml_kafka_topic,caml_kafka_partition,caml_opt_key,caml_msgid,caml_msg);
  CAMLlocal1(caml_key);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32_t partition = Int_val(caml_kafka_partition);

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
     RAISE(rd_errno, "Failed to produce message (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_outq_len(value caml_kafka_handler)
{
  CAMLparam1(caml_kafka_handler);
  CAMLlocal1(caml_len);

  rd_kafka_t *handler = get_handler(caml_kafka_handler);
  int len = rd_kafka_outq_len(handler);

  caml_len = Val_int(len);
  CAMLreturn(caml_len);
}

extern CAMLprim
value ocaml_kafka_poll(value caml_kafka_handler, value caml_kafka_timeout)
{
  CAMLparam2(caml_kafka_handler, caml_kafka_timeout);
  CAMLlocal1(caml_count);

  rd_kafka_t *handler = get_handler(caml_kafka_handler);
  int timeout = Int_val(caml_kafka_timeout);
  int count = rd_kafka_poll(handler, timeout);

  caml_count = Val_int(count);
  CAMLreturn(caml_count);
}

extern CAMLprim
value ocaml_kafka_store_offset(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_offset)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_kafka_offset);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32_t partition = Int_val(caml_kafka_partition);
  int64_t offset = Int64_val(caml_kafka_offset);

  rd_kafka_resp_err_t rd_errno = rd_kafka_offset_store(topic, partition, offset);
  if (rd_errno) {
     RAISE(rd_errno, "Failed to store offset (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

/**
  A rdkafka queue handler is wrapped with a list of OCaml topics,
  so on message consuption we can return a plain OCaml topic handler.

  caml_queue = block {
     field 0 : rd_kafka_queue = abstract tag
     field 1 : topics = caml list of topics
  }
          
**/

extern CAMLprim
value ocaml_kafka_new_queue(value caml_kafka_handler)
{
  CAMLparam1(caml_kafka_handler);
  CAMLlocal2(caml_queue, caml_kafka_queue_handler);

  rd_kafka_t *handler = get_handler(caml_kafka_handler);

  rd_kafka_queue_t* queue = rd_kafka_queue_new(handler);
  if (!queue) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
     RAISE(rd_errno, "Failed to create new kafka queue (%s)", rd_kafka_err2str(rd_errno));
  }

  caml_kafka_queue_handler = alloc_caml_handler(queue);
  caml_queue = caml_alloc_small(2,0);
  Field(caml_queue, 0) = caml_kafka_queue_handler;
  Field(caml_queue, 1) = Val_emptylist;

  CAMLreturn(caml_queue);
}

extern CAMLprim
value ocaml_kafka_destroy_queue(value caml_queue_handler)
{
  CAMLparam1(caml_queue_handler);

  rd_kafka_queue_t *queue = handler_val(Field(caml_queue_handler,0));
  if (queue) {
    rd_kafka_queue_destroy(queue);
    free_caml_handler(Field(caml_queue_handler,0));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_consume_start_queue(value caml_kafka_queue, value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_offset)
{
  CAMLparam4(caml_kafka_queue,caml_kafka_topic,caml_kafka_partition,caml_kafka_offset);
  CAMLlocal3(caml_topics, caml_topic, caml_cons);

  rd_kafka_queue_t *queue = get_handler(Field(caml_kafka_queue,0));
  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);

  rd_kafka_topic_t *found_topic = NULL;
  caml_topics = Field(caml_kafka_queue,1);
  while (found_topic == NULL && caml_topics != Val_emptylist) {
     caml_topic = Field(caml_topics,0);
     caml_topics = Field(caml_topics,1);
     rd_kafka_topic_t *handler = get_handler(caml_topic);
     if (handler == topic) {
       found_topic = handler;
     }
  }
  if (! found_topic) {
     caml_cons = caml_alloc_small(2,0);
     Field(caml_cons, 0) = caml_kafka_topic;
     Field(caml_cons, 1) = Field(caml_kafka_queue,1);
     Store_field(caml_kafka_queue,1,caml_cons);
  }

  int32_t partition = Int_val(caml_kafka_partition);
  int64_t offset = Int64_val(caml_kafka_offset);
  int err = rd_kafka_consume_start_queue(topic, partition, offset, queue);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
     RAISE(rd_errno, "Failed to start consuming & queue messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern
value ocaml_kafka_search_registered_topic(value caml_kafka_queue, rd_kafka_topic_t *topic)
{
  CAMLparam1(caml_kafka_queue);
  CAMLlocal2(caml_topics, caml_topic);

  rd_kafka_topic_t *found_topic = NULL;
  caml_topics = Field(caml_kafka_queue,1);
  while (found_topic == NULL && caml_topics != Val_emptylist) {
    caml_topic = Field(caml_topics,0);
    caml_topics = Field(caml_topics,1);
    rd_kafka_topic_t *handler = get_handler(caml_topic);
    if (handler == topic) {
      found_topic = handler;
    }
  }

  if (! found_topic) {
    RAISE(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, "Message received from un-registred topic");
  }

  CAMLreturn(caml_topic);
}

extern
value ocaml_kafka_extract_queue_message(value caml_kafka_queue, rd_kafka_message_t* message)
{
  CAMLparam1(caml_kafka_queue);
  CAMLlocal2(caml_topic, caml_msg);

  rd_kafka_topic_t *topic = message->rkt;
  caml_topic = ocaml_kafka_search_registered_topic(caml_kafka_queue, topic);
  caml_msg = ocaml_kafka_extract_topic_message(caml_topic, message);

  CAMLreturn(caml_msg);
}

extern
value ocaml_kafka_extract_queue_message_list(value caml_kafka_queue, rd_kafka_message_t** messages, size_t msg_count)
{
  CAMLparam1(caml_kafka_queue);
  CAMLlocal5(caml_topic, caml_msg, caml_msg_list, caml_new_cons, caml_last_cons);

  size_t i;
  caml_msg_list = Val_emptylist;
  for (i = 0; i<msg_count; ++i) {
    rd_kafka_message_t* message = messages[i];
    caml_topic = ocaml_kafka_search_registered_topic(caml_kafka_queue, message->rkt);
    caml_msg = ocaml_kafka_extract_topic_message(caml_topic, message);
    caml_new_cons = caml_alloc(2,0);
    Store_field(caml_new_cons, 0, caml_msg);
    Store_field(caml_new_cons, 1, Val_emptylist);
    if (i == 0) {
       caml_msg_list = caml_new_cons;
    } else {
       Store_field(caml_last_cons, 1, caml_new_cons);
    }
    caml_last_cons = caml_new_cons;
  }

  CAMLreturn(caml_msg_list);
}

extern CAMLprim
value ocaml_kafka_consume_queue(value caml_kafka_timeout, value caml_kafka_queue)
{
  CAMLparam2(caml_kafka_queue,caml_kafka_timeout);
  CAMLlocal1(caml_msg);

  rd_kafka_queue_t *queue = get_handler(Field(caml_kafka_queue,0));
  int timeout = DEFAULT_TIMEOUT_MS;
  if (Is_block(caml_kafka_timeout)) {
    int t = Int_val(Field(caml_kafka_timeout, 0));
    timeout = t>=0 ? t : DEFAULT_TIMEOUT_MS;
  }

  rd_kafka_message_t* message = rd_kafka_consume_queue(queue, timeout);

  if (message) {
     caml_msg = ocaml_kafka_extract_queue_message(caml_kafka_queue, message);
     rd_kafka_message_destroy(message);
  } else {
     rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
     RAISE(rd_errno, "Failed to consume message from queue (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(caml_msg);
}

extern CAMLprim
value ocaml_kafka_consume_batch_queue(value caml_kafka_timeout, value caml_msg_count, value caml_kafka_queue)
{
  CAMLparam3(caml_kafka_queue,caml_kafka_timeout, caml_msg_count);
  CAMLlocal1(caml_msg_list);

  rd_kafka_queue_t *queue = get_handler(Field(caml_kafka_queue,0));
  int timeout = DEFAULT_TIMEOUT_MS;
  if (Is_block(caml_kafka_timeout)) {
    int t = Int_val(Field(caml_kafka_timeout, 0));
    timeout = t>=0 ? t : DEFAULT_TIMEOUT_MS;
  }
  size_t msg_count = DEFAULT_MSG_COUNT;
  if (Is_block(caml_msg_count)) {
    int n = Int_val(Field(caml_msg_count, 0));
    msg_count = n>=0 ? n : 0;
  }

  rd_kafka_message_t* messages[msg_count];

  ssize_t actual_msg_count = rd_kafka_consume_batch_queue(queue, timeout, messages, msg_count);

  if (actual_msg_count >= 0) {
    caml_msg_list = ocaml_kafka_extract_queue_message_list(caml_kafka_queue, messages, actual_msg_count);
    size_t i;
    for (i = 0; i<actual_msg_count; ++i) {
      rd_kafka_message_destroy(messages[i]);
    }
  } else {
    rd_kafka_resp_err_t rd_errno = rd_kafka_last_error();
    RAISE(rd_errno, "Failed to consume messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(caml_msg_list);
}

static
value make_topic_metadata(rd_kafka_metadata_topic_t *topic)
{
  CAMLparam0();
  CAMLlocal4(caml_topic_metadata,caml_topic_name,caml_partitions,caml_cons);

  caml_partitions = Val_emptylist;
  int i,n;
  n = topic->partition_cnt;
  rd_kafka_metadata_partition_t *partition = topic->partitions;
  for(i=0;i<n;++i,++partition) {
     caml_cons = caml_alloc_small(2,0);
     Field(caml_cons, 0) = Val_int(partition->id);
     Field(caml_cons, 1) = caml_partitions;
     caml_partitions = caml_cons;
  }

  caml_topic_name = caml_copy_string(topic->topic);
  caml_topic_metadata = caml_alloc_small(2, 0);
  Field(caml_topic_metadata, 0) = caml_topic_name;
  Field(caml_topic_metadata, 1) = caml_partitions;

  CAMLreturn(caml_topic_metadata);
}

extern CAMLprim
value ocaml_kafka_get_topic_metadata(value caml_handler, value caml_topic, value caml_timeout)
{
  CAMLparam3(caml_handler,caml_topic,caml_timeout);
  CAMLlocal1(caml_topic_metadata);

  rd_kafka_t *handler = get_handler(caml_handler);
  rd_kafka_topic_t *topic = get_handler(caml_topic);
  int timeout = Int_val(caml_timeout);

  const struct rd_kafka_metadata *metadata = NULL;
  rd_kafka_resp_err_t err = rd_kafka_metadata (handler, 0, topic, &metadata, timeout);
  if (err) {
     RAISE(err, "Failed to fetch topic metadata (%s)", rd_kafka_err2str(err));
  }
  if (!metadata || metadata->topic_cnt != 1) {
     RAISE(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, rd_kafka_err2str(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC));
  }
  else {
     caml_topic_metadata = make_topic_metadata(metadata->topics);
  }
  if (metadata) {
     rd_kafka_metadata_destroy(metadata);
  }

  CAMLreturn(caml_topic_metadata);
}

extern CAMLprim
value ocaml_kafka_get_topics_metadata(value caml_handler, value caml_all_topics, value caml_timeout)
{
  CAMLparam3(caml_handler,caml_all_topics,caml_timeout);
  CAMLlocal3(caml_topics_metadata,caml_topic_metadata,caml_cons);

  rd_kafka_t *handler = get_handler(caml_handler);
  int all_topics = Bool_val(caml_all_topics);
  int timeout = Int_val(caml_timeout);

  const struct rd_kafka_metadata *metadata = NULL;
  rd_kafka_resp_err_t err = rd_kafka_metadata (handler, all_topics, NULL, &metadata, timeout);
  if (err) {
     RAISE(err, "Failed to fetch topics metadata (%s)", rd_kafka_err2str(err));
  }

  caml_topics_metadata = Val_emptylist;
  if (metadata) {
     int i,n;
     n = metadata->topic_cnt;
     rd_kafka_metadata_topic_t *topic = metadata->topics;
     for(i=0;i<n;++i,++topic) {
        caml_topic_metadata = make_topic_metadata(topic);

        caml_cons = caml_alloc_small(2,0);
        Field(caml_cons, 0) = caml_topic_metadata;
        Field(caml_cons, 1) = caml_topics_metadata;
        caml_topics_metadata = caml_cons;
     }
     rd_kafka_metadata_destroy(metadata);
  }

  CAMLreturn(caml_topics_metadata);
}
