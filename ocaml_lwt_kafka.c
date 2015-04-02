#include <caml/memory.h>
#include <caml/alloc.h>
#include <lwt_unix.h>
#include <errno.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

#define RAISE ocaml_kafka_raise
extern void ocaml_kafka_raise(rd_kafka_resp_err_t rd_errno, const char *error, ...);
extern value ocaml_kafka_extract_topic_message(value caml_kafka_topic, rd_kafka_message_t* message);
extern value ocaml_kafka_extract_queue_message(value caml_kafka_queue, rd_kafka_message_t* message);

#define handler_val(v) *((void **) &Field(v, 0))
inline static void* get_handler(value caml_handler)
{
  void* hdl = handler_val(caml_handler);
  if (! hdl) {
    RAISE(RD_KAFKA_RESP_ERR__INVALID_ARG, "Kafka handler has been released");
  }

  return hdl;
}

/****************************************
 * Kafka_lwt.consume
 ***************************************/

/* Structure holding informations for calling [consume]. */
struct job_consume {
  struct lwt_unix_job job;

  rd_kafka_topic_t *topic;
  int32 partition;
  int timeout;

  value caml_kafka_topic; /* We hide the topic in the job, so we can attach it to message. */

  rd_kafka_message_t* message;
  rd_kafka_resp_err_t rd_errno;
};

/* The function calling [consume]. */
static void worker_consume(struct job_consume* job)
{
  job->message = rd_kafka_consume(job->topic, job->partition, job->timeout);
  job->rd_errno = (job->message)?RD_KAFKA_RESP_ERR_NO_ERROR:rd_kafka_errno2err(errno);
}

/* The function building the caml [consume] result. */
static value result_consume(struct job_consume* job)
{
  CAMLparam0();
  CAMLlocal1(caml_msg);

  rd_kafka_message_t* message = job->message;
  if (message) {
    caml_msg = ocaml_kafka_extract_topic_message(job->caml_kafka_topic, message);
    rd_kafka_message_destroy(message);
  } else {
    rd_kafka_resp_err_t rd_errno = job->rd_errno;
    RAISE(rd_errno, "Failed to consume message (%s)", rd_kafka_err2str(rd_errno));
  }

  lwt_unix_free_job(&job->job);

  CAMLreturn(caml_msg);
}

/* The stub creating the [consume] job structure. */
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

/****************************************
 * Kafka_lwt.consume_queue
 ***************************************/

/* Structure holding informations for calling [consume_queue]. */
struct job_consume_queue {
  struct lwt_unix_job job;

  rd_kafka_queue_t *queue;
  int timeout;

  value caml_kafka_queue; /* We hide the queue in the job, so we can search caml handler of topics. */

  rd_kafka_message_t* message;
  rd_kafka_resp_err_t rd_errno;
};

/* The function calling [consume_queue]. */
static void worker_consume_queue(struct job_consume_queue* job)
{
  job->message = rd_kafka_consume_queue(job->queue, job->timeout);
  job->rd_errno = (job->message)?RD_KAFKA_RESP_ERR_NO_ERROR:rd_kafka_errno2err(errno);
}

/* The function building the caml [consume_queue] result. */
static value result_consume_queue(struct job_consume_queue* job)
{
  CAMLparam0();
  CAMLlocal1(caml_msg);

  rd_kafka_message_t* message = job->message;

  if (message) {
     caml_msg = ocaml_kafka_extract_queue_message(job->caml_kafka_queue, message);
     rd_kafka_message_destroy(message);
  } else {
     rd_kafka_resp_err_t rd_errno = rd_kafka_errno2err(errno);
     RAISE(rd_errno, "Failed to consume message from queue (%s)", rd_kafka_err2str(rd_errno));
  }

  lwt_unix_free_job(&job->job);

  CAMLreturn(caml_msg);
}

/* The stub creating the [consume_queue] job structure. */
extern CAMLprim
value ocaml_kafka_consume_queue_job(value caml_kafka_queue, value caml_kafka_timeout)
{
  struct job_consume_queue* job = (struct job_consume_queue*)lwt_unix_new(struct job_consume_queue);

  job->queue = get_handler(Field(caml_kafka_queue,0));
  job->timeout = Int_val(caml_kafka_timeout);
  job->caml_kafka_queue = caml_kafka_queue;

  job->job.worker = (lwt_unix_job_worker)worker_consume_queue;
  job->job.result = (lwt_unix_job_result)result_consume_queue;

  return lwt_unix_alloc_job(&job->job);
}
