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
  CAMLlocal2(caml_topic, caml_kafka_topic_handler);

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
  caml_kafka_topic_handler = alloc_caml_handler(topic);
  caml_topic = caml_alloc_small(2,0);
  Field(caml_topic, 0) = caml_kafka_topic_handler;
  Field(caml_topic, 1) = caml_topic_name;
  CAMLreturn(caml_topic);
}

extern CAMLprim
value ocaml_kafka_destroy_topic(value caml_kafka_topic)
{
  CAMLparam1(caml_kafka_topic);

  rd_kafka_topic_t *topic = handler_val(Field(caml_kafka_topic,0));
  if (topic) {
    free_caml_handler(Field(caml_kafka_topic,0));
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
  CAMLlocal3(caml_msg, caml_msg_payload, caml_msg_offset);

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

     caml_msg_offset = caml_copy_int64(message->offset);
     caml_msg = caml_alloc_small(4, 0);
     Field( caml_msg, 0) = caml_kafka_topic;
     Field( caml_msg, 1) = caml_kafka_partition;
     Field( caml_msg, 2) = caml_msg_offset;
     Field( caml_msg, 3) = caml_msg_payload;
  }
  else if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
     caml_msg_offset = caml_copy_int64(message->offset);
     caml_msg = caml_alloc_small(3, 1);
     Field( caml_msg, 0) = caml_kafka_topic;
     Field( caml_msg, 1) = caml_kafka_partition;
     Field( caml_msg, 2) = caml_msg_offset;
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
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
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
  rd_kafka_topic_t *topic = get_handler(Field(caml_kafka_topic,0));

  rd_kafka_topic_t *found_topic = NULL;
  caml_topics = Field(caml_kafka_queue,1);
  while (found_topic == NULL && caml_topics != Val_emptylist) {
     caml_topic = Field(caml_topics,0);
     caml_topics = Field(caml_topics,1);
     rd_kafka_topic_t *handler = get_handler(Field(caml_topic,0));
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
  CAMLlocal5(caml_topics, caml_topic, caml_msg, caml_msg_payload, caml_msg_offset);

  rd_kafka_queue_t *queue = get_handler(Field(caml_kafka_queue,0));
  int timeout = Int_val(caml_kafka_timeout);
  rd_kafka_message_t* message = rd_kafka_consume_queue(queue, timeout);

  if (message == NULL) {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to consume message from queue (%s)", rd_kafka_err2str(rd_errno));
  }
  else {
     rd_kafka_topic_t *topic = message->rkt;
     rd_kafka_topic_t *found_topic = NULL;
     caml_topics = Field(caml_kafka_queue,1);
     while (found_topic == NULL && caml_topics != Val_emptylist) {
       caml_topic = Field(caml_topics,0);
       caml_topics = Field(caml_topics,1);
       rd_kafka_topic_t *handler = get_handler(Field(caml_topic,0));
       if (handler == topic) {
         found_topic = handler;
       }
     }

     if (found_topic) {
        if (!message->err) {
           caml_msg_payload = caml_alloc_string(message->len);
           memcpy(String_val(caml_msg_payload), message->payload, message->len);
           caml_msg_offset = caml_copy_int64(message->offset) ;
   
           caml_msg = caml_alloc_small(4, 0);
           Field( caml_msg, 0) = caml_topic;
           Field( caml_msg, 1) = Val_int(message->partition);
           Field( caml_msg, 2) = caml_msg_offset;
           Field( caml_msg, 3) = caml_msg_payload;
        }
        else if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
           caml_msg_offset = caml_copy_int64(message->offset) ;
           caml_msg = caml_alloc_small(3, 1);
           Field( caml_msg, 0) = caml_topic;
           Field( caml_msg, 1) = Val_int(message->partition);
           Field( caml_msg, 2) = caml_msg_offset;
        }
        else {
           if (message->payload) {
              RAISE(message->err, "Consumed message from queue with error (%s)", (const char *)message->payload);
           } else {
              RAISE(message->err, "Consumed message from queue with error (%s)", rd_kafka_err2str(message->err));
           }
        }
     } else {
        RAISE(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, "Message received from un-registred topic");
     }
  }

  if (message) {
     rd_kafka_message_destroy(message);
  }
  CAMLreturn(caml_msg);
}

static
value make_topic_metadata(rd_kafka_metadata_topic_t *topic)
{
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

  return caml_topic_metadata;
}

extern CAMLprim
value ocaml_kafka_get_topic_metadata(value caml_handler, value caml_topic, value caml_timeout)
{
  CAMLparam3(caml_handler,caml_topic,caml_timeout);
  CAMLlocal1(caml_topic_metadata);

  rd_kafka_t *handler = get_handler(caml_handler);
  rd_kafka_topic_t *topic = get_handler(Field(caml_topic,0));
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
