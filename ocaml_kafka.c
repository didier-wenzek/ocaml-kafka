#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <caml/alloc.h>

#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <librdkafka/rdkafka.h>

/* Configuration results (of type rd_kafka_conf_res_t) are merge with errors (of type rd_kafka_resp_err_t).
   So we have a single Error type ocaml side. */
#define RD_KAFKA_CONF_RES(kafka_conf_res) (RD_KAFKA_RESP_ERR__END + kafka_conf_res)

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

static
void RAISE(rd_kafka_resp_err_t rd_errno, const char *error, ...)
{
  CAMLlocalN(error_parameters, 2);
  static value *exception_handler = NULL;
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

  int caml_errno = RD_KAFKA_RESP_ERR_UNKNOWN;
  int i;
  for (i = 0; i < EUNKNOWN; i++) {
    if (rd_errno == ERROR_CODES[i]) {
      caml_errno = i;
      break;
    }
  }

  error_parameters[0] = Val_int(caml_errno);
  error_parameters[1] = caml_copy_string(error_msg);
  caml_raise_with_args(*exception_handler, 2, error_parameters);
}

#define handler_val(v) *((void **) &Field(v, 0))

inline static value alloc_caml_handler(void* hdl)
{
  value caml_handler = alloc_small(1, Abstract_tag);
  handler_val(caml_handler) = hdl;
  return caml_handler;
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

static
rd_kafka_conf_res_t configure_handler(rd_kafka_conf_t *conf, value caml_options, char *errstr, size_t errstr_size)
{
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
  return RD_KAFKA_CONF_OK;
};

static
rd_kafka_conf_res_t configure_topic(rd_kafka_topic_conf_t *conf, value caml_options, char *errstr, size_t errstr_size)
{
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
  return RD_KAFKA_CONF_OK;
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
     RAISE(RD_KAFKA_RESP_ERR__FAIL, "Failed to create new kafka consumer (%s)", error_msg);
  }

  value caml_handler = alloc_caml_handler(handler);
  CAMLreturn(caml_handler);
}

extern CAMLprim
value ocaml_kafka_new_producer(value caml_producer_options)
{
  CAMLparam1(caml_producer_options);

  char error_msg[160];
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_res_t conf_err = configure_handler(conf, caml_producer_options, error_msg, sizeof(error_msg));
  if (conf_err) {
     rd_kafka_conf_destroy(conf);
     RAISE(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka producer (%s)", error_msg);
  }

  rd_kafka_t *handler = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error_msg, sizeof(error_msg));
  if (handler == NULL) {
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

extern CAMLprim
value ocaml_kafka_new_topic(value caml_kafka_handler, value caml_topic_name, value caml_topic_options)
{
  CAMLparam3(caml_kafka_handler, caml_topic_name, caml_topic_options);
  CAMLlocal1(caml_topic);

  rd_kafka_t *handler = get_handler(caml_kafka_handler);
  const char* name = String_val(caml_topic_name);

  char error_msg[160];
  rd_kafka_topic_conf_t *conf = rd_kafka_topic_conf_new();
  rd_kafka_conf_res_t conf_err = configure_topic(conf, caml_topic_options, error_msg, sizeof(error_msg));
  if (conf_err) {
     rd_kafka_topic_conf_destroy(conf);
     RAISE(RD_KAFKA_CONF_RES(conf_err), "Failed to configure new kafka topic (%s)", error_msg);
  }

  rd_kafka_topic_t* topic = rd_kafka_topic_new(handler, name, conf);
  if (!topic) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to create new kafka topic (%s)", rd_kafka_err2str(rd_errno));
  }

  // The handler is wrapped with its name.
  caml_topic = caml_alloc(2,0);
  Store_field(caml_topic, 0, alloc_caml_handler(topic));
  Store_field(caml_topic, 1, caml_topic_name);
  CAMLreturn(caml_topic);
}

extern CAMLprim
value ocaml_kafka_destroy_topic(value caml_kafka_topic)
{
  CAMLparam1(caml_kafka_topic);

  rd_kafka_topic_t *topic = handler_val(Field(caml_kafka_topic,0));
  if (topic) {
    free_caml_handler(caml_kafka_topic);
    rd_kafka_topic_destroy(topic);
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_topic_name(value caml_kafka_topic)
{
  CAMLparam1(caml_kafka_topic);
  CAMLlocal1(caml_name);

  caml_name = Field(caml_kafka_topic,1);

  CAMLreturn(caml_name);
}

extern CAMLprim
value ocaml_kafka_topic_partition_available(value caml_kafka_topic, value caml_kafka_partition)
{
  CAMLparam2(caml_kafka_topic, caml_kafka_partition);

  rd_kafka_topic_t *topic = get_handler(caml_kafka_topic);
  int32 partition = Int_val(caml_kafka_partition);
  
  if (rd_kafka_topic_partition_available(topic, partition)) {
     CAMLreturn(Val_true);
  } else {
     CAMLreturn(Val_false);
  }
}

extern CAMLprim
value ocaml_kafka_consume_start(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_offset)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_kafka_offset);

  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));
  int32 partition = Int_val(caml_kafka_partition);
  int64 offset = Int64_val(caml_kafka_offset);
  int err = rd_kafka_consume_start(topic, partition, offset);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to start consuming messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_consume_stop(value caml_kafka_topic, value caml_kafka_partition)
{
  CAMLparam2(caml_kafka_topic,caml_kafka_partition);

  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));
  int32 partition = Int_val(caml_kafka_partition);
  int err = rd_kafka_consume_stop(topic, partition);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to stop consuming messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_consume(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_timeout)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_kafka_timeout);
  CAMLlocal2(caml_msg, caml_msg_payload);

  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));
  int32 partition = Int_val(caml_kafka_partition);
  int timeout = Int_val(caml_kafka_timeout);
  rd_kafka_message_t* message = rd_kafka_consume(topic, partition, timeout);

  if (message == NULL) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to consume message (%s)", rd_kafka_err2str(rd_errno));
  }
  else if (!message->err) {
     caml_msg_payload = caml_alloc_string(message->len);
     memcpy(String_val(caml_msg_payload), message->payload, message->len);

     caml_msg = caml_alloc(4, 0);
     Store_field( caml_msg, 0, caml_kafka_topic );
     Store_field( caml_msg, 1, caml_kafka_partition );
     Store_field( caml_msg, 2, caml_copy_int64(message->offset) );
     Store_field( caml_msg, 3, caml_msg_payload );
  }
  else if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
     caml_msg = caml_alloc(3, 1);
     Store_field( caml_msg, 0, caml_kafka_topic );
     Store_field( caml_msg, 1, caml_kafka_partition );
     Store_field( caml_msg, 2, caml_copy_int64(message->offset) );
  }
  else {
     if (message->payload) {
        RAISE(message->err, "Consumed message with error (%s)", (const char *)message->payload);
     } else {
        RAISE(message->err, "Consumed message with error (%s)", rd_kafka_err2str(message->err));
     }
  }

  if (message) {
     rd_kafka_message_destroy(message);
  }
  CAMLreturn(caml_msg);
}

extern CAMLprim
value ocaml_kafka_produce(value caml_kafka_topic, value caml_kafka_partition, value caml_msg)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_msg);

  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));
  int32 partition = Int_val(caml_kafka_partition);
  void* payload = String_val(caml_msg);
  size_t len = caml_string_length(caml_msg);

  int err = rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY, payload, len, NULL, 0, NULL);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to produce message (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_store_offset(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_offset)
{
  CAMLparam3(caml_kafka_topic,caml_kafka_partition,caml_kafka_offset);

  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));
  int32 partition = Int_val(caml_kafka_partition);
  int64 offset = Int64_val(caml_kafka_offset);

  rd_kafka_resp_err_t rd_errno = rd_kafka_offset_store(topic, partition, offset);
  if (rd_errno) {
     RAISE(rd_errno, "Failed to store offset (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

/**
  A rdkafka queue handler is wrapped with a list of OCaml topics,
  so on message consuption we can return a plain OCaml topic handler.
**/
typedef struct chained_topic {
   rd_kafka_topic_t     *topic;
   struct chained_topic *next;
   value                caml_topic;
} chained_topic_t;

typedef struct queue_topics {
   rd_kafka_queue_t  *queue;
   chained_topic_t   *topics;
} queue_topics_t;

extern CAMLprim
value ocaml_kafka_new_queue(value caml_kafka_handler)
{
  CAMLparam1(caml_kafka_handler);
  CAMLlocal1(caml_queue);

  rd_kafka_t *handler = get_handler(caml_kafka_handler);

  rd_kafka_queue_t* queue = rd_kafka_queue_new(handler);
  if (!queue) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to create new kafka queue (%s)", rd_kafka_err2str(rd_errno));
  }

  queue_topics_t *queue_topics = malloc(sizeof(queue_topics_t));
  if (!queue_topics) {
     RAISE(RD_KAFKA_RESP_ERR_UNKNOWN, "Failed to allocate new kafka queue");
  }

  queue_topics->queue = queue;
  queue_topics->topics = NULL;

  caml_queue = alloc_caml_handler(queue_topics);
  CAMLreturn(caml_queue);
}

extern CAMLprim
value ocaml_kafka_destroy_queue(value caml_queue_handler)
{
  CAMLparam1(caml_queue_handler);

  queue_topics_t *queue_topics = handler_val(caml_queue_handler);
  if (queue_topics) {
    rd_kafka_queue_destroy(queue_topics->queue);
    
    chained_topic_t *chained_topic = queue_topics->topics;
    while (chained_topic) {
       chained_topic_t *topic = chained_topic;
       chained_topic = topic->next;
       free(topic);
    }

    free_caml_handler(caml_queue_handler);
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_consume_start_queue(value caml_kafka_queue, value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_offset)
{
  CAMLparam4(caml_kafka_queue,caml_kafka_topic,caml_kafka_partition,caml_kafka_offset);

  queue_topics_t *queue_topics = handler_val(caml_kafka_queue);
  rd_kafka_queue_t* queue = queue_topics->queue;
  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));

  chained_topic_t *chained_topic = queue_topics->topics;
  while (chained_topic && chained_topic->topic != topic) {
     chained_topic = chained_topic->next;
  }
  if (! chained_topic) {
     chained_topic = malloc(sizeof(chained_topic_t));
     if (chained_topic) {
        chained_topic->topic = topic;
        chained_topic->caml_topic = caml_kafka_topic;
        chained_topic->next = queue_topics->topics;
        queue_topics->topics = chained_topic;
     } else {
        RAISE(RD_KAFKA_RESP_ERR_UNKNOWN, "Failed to register topic in queue");
     }
  }

  int32 partition = Int_val(caml_kafka_partition);
  int64 offset = Int64_val(caml_kafka_offset);
  int err = rd_kafka_consume_start_queue(topic, partition, offset, queue);
  if (err) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to start consuming & queue messages (%s)", rd_kafka_err2str(rd_errno));
  }

  CAMLreturn(Val_unit);
}

extern CAMLprim
value ocaml_kafka_consume_queue(value caml_kafka_queue, value caml_kafka_timeout)
{
  CAMLparam2(caml_kafka_queue,caml_kafka_timeout);
  CAMLlocal2(caml_msg, caml_msg_payload);

  queue_topics_t *queue_topics = handler_val(caml_kafka_queue);
  rd_kafka_queue_t* queue = queue_topics->queue;
  int timeout = Int_val(caml_kafka_timeout);
  rd_kafka_message_t* message = rd_kafka_consume_queue(queue, timeout);

  if (message == NULL) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to consume message from queue (%s)", rd_kafka_err2str(rd_errno));
  }
  else {
     rd_kafka_topic_t *topic = message->rkt;
     chained_topic_t *chained_topic = queue_topics->topics;
     while (chained_topic && chained_topic->topic != topic) {
       chained_topic = chained_topic->next;
     }

     if (chained_topic && !message->err) {
        caml_msg_payload = caml_alloc_string(message->len);
        memcpy(String_val(caml_msg_payload), message->payload, message->len);
   
        caml_msg = caml_alloc(4, 0);
        Store_field( caml_msg, 0, chained_topic->caml_topic );
        Store_field( caml_msg, 1, Val_int(message->partition) );
        Store_field( caml_msg, 2, caml_copy_int64(message->offset) );
        Store_field( caml_msg, 3, caml_msg_payload );
     }
     else if (chained_topic && message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        caml_msg = caml_alloc(3, 1);
        Store_field( caml_msg, 0, chained_topic->caml_topic );
        Store_field( caml_msg, 1, Val_int(message->partition) );
        Store_field( caml_msg, 2, caml_copy_int64(message->offset) );
     }
     else if (chained_topic) {
        if (message->payload) {
           RAISE(message->err, "Consumed message from queue with error (%s)", (const char *)message->payload);
        } else {
           RAISE(message->err, "Consumed message from queue with error (%s)", rd_kafka_err2str(message->err));
        }
     }
     else {
        RAISE(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, "Message received from un-registred topic");
     }
  }

  if (message) {
     rd_kafka_message_destroy(message);
  }
  CAMLreturn(caml_msg);
}
