#include <caml/memory.h>
#include <caml/alloc.h>
#include <lwt_unix.h>
#include <errno.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

#define RAISE ocaml_kafka_raise
extern
void ocaml_kafka_raise(rd_kafka_resp_err_t rd_errno, const char *error, ...);

#define handler_val(v) *((void **) &Field(v, 0))
inline static void* get_handler(value caml_handler)
{
  void* hdl = handler_val(caml_handler);
  if (! hdl) {
    RAISE(RD_KAFKA_RESP_ERR__INVALID_ARG, "Kafka handler has been released");
  }

  return hdl;
}

/* Structure holding informations for calling [consume]. */
struct job_consume {
  struct lwt_unix_job job;

  rd_kafka_topic_t *topic;
  int32 partition;
  int timeout;

  value caml_kafka_topic; /* We hide the former topic in the job, to return it in the message */

  rd_kafka_message_t* message;
  rd_kafka_resp_err_t rd_errno;
};

/* The function calling [consume]. */
static void worker_consume(struct job_consume* job)
{
  job->message = rd_kafka_consume(job->topic, job->partition, job->timeout);
  job->rd_errno = (job->message)?RD_KAFKA_RESP_ERR_NO_ERROR:rd_kafka_errno2err(errno);
}

/* The function building the caml result. */
static value result_consume(struct job_consume* job)
{
  CAMLparam0();
  CAMLlocal3(caml_msg, caml_msg_payload, caml_msg_offset);
  CAMLlocal2(caml_key, caml_key_payload);

  rd_kafka_message_t* message = job->message;

  if (message == NULL) {
     rd_kafka_resp_err_t rd_errno = job->rd_errno;
     RAISE(rd_errno, "Failed to consume message (%s)", rd_kafka_err2str(rd_errno));
  }
  else if (!message->err) {
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
     Field( caml_msg, 0) = job->caml_kafka_topic;
     Field( caml_msg, 1) = Val_int(message->partition);
     Field( caml_msg, 2) = caml_msg_offset;
     Field( caml_msg, 3) = caml_msg_payload;
     Field( caml_msg, 4) = caml_key;
  }
  else if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
     caml_msg_offset = caml_copy_int64(message->offset);
     caml_msg = caml_alloc_small(3, 1);
     Field( caml_msg, 0) = job->caml_kafka_topic;
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

  if (message) {
     rd_kafka_message_destroy(message);
  }
  lwt_unix_free_job(&job->job);

  CAMLreturn(caml_msg);
}

/* The stub creating the job structure. */
extern CAMLprim
value ocaml_kafka_consume_job(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_timeout)
{
  struct job_consume* job = (struct job_consume*)lwt_unix_new(struct job_consume);

  job->caml_kafka_topic = caml_kafka_topic;
  job->topic = get_handler(Field(caml_kafka_topic,0));
  job->partition = Int_val(caml_kafka_partition);
  job->timeout = Int_val(caml_kafka_timeout);

  job->job.worker = (lwt_unix_job_worker)worker_consume;
  job->job.result = (lwt_unix_job_result)result_consume;

  return lwt_unix_alloc_job(&job->job);
}
