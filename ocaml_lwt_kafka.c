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
extern value ocaml_kafka_extract_topic_message_list(value caml_kafka_topic, rd_kafka_message_t** messages, size_t msg_count);
extern value ocaml_kafka_extract_queue_message_list(value caml_kafka_queue, rd_kafka_message_t** messages, size_t msg_count);

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
  int32_t partition;
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
    caml_remove_generational_global_root(&(job->caml_kafka_topic));
    lwt_unix_free_job(&job->job);
    RAISE(rd_errno, "Failed to consume message (%s)", rd_kafka_err2str(rd_errno));
  }

  caml_remove_generational_global_root(&(job->caml_kafka_topic));
  lwt_unix_free_job(&job->job);
  CAMLreturn(caml_msg);
}

/* The stub creating the [consume] job structure. */
extern CAMLprim
value ocaml_kafka_consume_job(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_timeout)
{
  CAMLparam3(caml_kafka_topic, caml_kafka_partition, caml_kafka_timeout);
  struct job_consume* job = (struct job_consume*)lwt_unix_new(struct job_consume);

  job->caml_kafka_topic = caml_kafka_topic;
  caml_register_generational_global_root(&(job->caml_kafka_topic));
  job->topic = get_handler(Field(caml_kafka_topic,0));
  job->partition = Int_val(caml_kafka_partition);
  job->timeout = Int_val(caml_kafka_timeout);

  job->job.worker = (lwt_unix_job_worker)worker_consume;
  job->job.result = (lwt_unix_job_result)result_consume;

  CAMLreturn(lwt_unix_alloc_job(&job->job));
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
     caml_remove_generational_global_root(&(job->caml_kafka_queue));
     lwt_unix_free_job(&job->job);
     RAISE(rd_errno, "Failed to consume message from queue (%s)", rd_kafka_err2str(rd_errno));
  }

  caml_remove_generational_global_root(&(job->caml_kafka_queue));
  lwt_unix_free_job(&job->job);
  CAMLreturn(caml_msg);
}

/* The stub creating the [consume_queue] job structure. */
extern CAMLprim
value ocaml_kafka_consume_queue_job(value caml_kafka_queue, value caml_kafka_timeout)
{
  CAMLparam2(caml_kafka_queue, caml_kafka_timeout);
  struct job_consume_queue* job = (struct job_consume_queue*)lwt_unix_new(struct job_consume_queue);

  job->queue = get_handler(Field(caml_kafka_queue,0));
  job->timeout = Int_val(caml_kafka_timeout);
  job->caml_kafka_queue = caml_kafka_queue;
  caml_register_generational_global_root(&(job->caml_kafka_queue));

  job->job.worker = (lwt_unix_job_worker)worker_consume_queue;
  job->job.result = (lwt_unix_job_result)result_consume_queue;

  CAMLreturn(lwt_unix_alloc_job(&job->job));
}

/****************************************
 * Kafka_lwt.consume_batch
 ***************************************/

/* Structure holding informations for calling [consume_batch]. */
struct job_consume_batch {
  struct lwt_unix_job job;

  rd_kafka_topic_t *topic;
  int32_t partition;
  int timeout;
  size_t msg_count;

  value caml_kafka_topic;         // We hide the topic in the job, so we can attach it to messages.

  ssize_t actual_msg_count;
  rd_kafka_resp_err_t rd_errno;
  rd_kafka_message_t* messages[]; // Buffer for messages.
};

/* The function calling [consume_batch]. */
static void worker_consume_batch(struct job_consume_batch* job)
{
  job->actual_msg_count = rd_kafka_consume_batch(job->topic, job->partition, job->timeout, job->messages, job->msg_count);
  job->rd_errno = (job->actual_msg_count>=0)?RD_KAFKA_RESP_ERR_NO_ERROR:rd_kafka_errno2err(errno);
}

/* The function building the caml [consume_batch] result. */
static value result_consume_batch(struct job_consume_batch* job)
{
  CAMLparam0();
  CAMLlocal1(caml_msg_list);

  ssize_t actual_msg_count = job->actual_msg_count;
  if (actual_msg_count >= 0) {
    caml_msg_list = ocaml_kafka_extract_topic_message_list(job->caml_kafka_topic, job->messages, actual_msg_count);
    size_t i;
    for (i = 0; i<actual_msg_count; ++i) {
      rd_kafka_message_destroy(job->messages[i]);
    }
  } else {
    rd_kafka_resp_err_t rd_errno = job->rd_errno;
    caml_remove_generational_global_root(&(job->caml_kafka_topic));
    lwt_unix_free_job(&job->job);
    RAISE(rd_errno, "Failed to consume messages (%s)", rd_kafka_err2str(rd_errno));
  }

  caml_remove_generational_global_root(&(job->caml_kafka_topic));
  lwt_unix_free_job(&job->job);
  CAMLreturn(caml_msg_list);
}

/* The stub creating the [consume_batch] job structure. */
extern CAMLprim
value ocaml_kafka_consume_batch_job(value caml_kafka_topic, value caml_kafka_partition, value caml_kafka_timeout, value caml_msg_count)
{
  CAMLparam4(caml_kafka_topic, caml_kafka_partition, caml_kafka_timeout, caml_msg_count);
  int msg_count = Int_val(caml_msg_count);
  if (msg_count < 0) msg_count = 0;

  struct job_consume_batch* job = (struct job_consume_batch*)
    lwt_unix_new_plus(struct job_consume_batch, msg_count * sizeof (rd_kafka_message_t*));

  job->caml_kafka_topic = caml_kafka_topic;
  caml_register_generational_global_root(&(job->caml_kafka_topic));
  job->topic = get_handler(Field(caml_kafka_topic,0));
  job->partition = Int_val(caml_kafka_partition);
  job->timeout = Int_val(caml_kafka_timeout);
  job->msg_count = msg_count;

  job->job.worker = (lwt_unix_job_worker)worker_consume_batch;
  job->job.result = (lwt_unix_job_result)result_consume_batch;

  CAMLreturn(lwt_unix_alloc_job(&job->job));
}

/****************************************
 * Kafka_lwt.consume_batch_queue
 ***************************************/

/* Structure holding informations for calling [consume_batch_queue]. */
struct job_consume_batch_queue {
  struct lwt_unix_job job;

  rd_kafka_queue_t *queue;
  int timeout;
  size_t msg_count;

  value caml_kafka_queue;         // We hide the queue in the job, so we can search caml handler of topics.

  ssize_t actual_msg_count;
  rd_kafka_resp_err_t rd_errno;
  rd_kafka_message_t* messages[]; // Buffer for messages.
};

/* The function calling [consume_batch_queue]. */
static void worker_consume_batch_queue(struct job_consume_batch_queue* job)
{
  job->actual_msg_count = rd_kafka_consume_batch_queue(job->queue, job->timeout, job->messages, job->msg_count);
  job->rd_errno = (job->actual_msg_count>=0)?RD_KAFKA_RESP_ERR_NO_ERROR:rd_kafka_errno2err(errno);
}

/* The function building the caml [consume_batch_queue] result. */
static value result_consume_batch_queue(struct job_consume_batch_queue* job)
{
  CAMLparam0();
  CAMLlocal1(caml_msg_list);

  ssize_t actual_msg_count = job->actual_msg_count;
  if (actual_msg_count >= 0) {
    caml_msg_list = ocaml_kafka_extract_queue_message_list(job->caml_kafka_queue, job->messages, actual_msg_count);
    size_t i;
    for (i = 0; i<actual_msg_count; ++i) {
      rd_kafka_message_destroy(job->messages[i]);
    }
  } else {
    rd_kafka_resp_err_t rd_errno = job->rd_errno;
    caml_remove_generational_global_root(&(job->caml_kafka_queue));
    lwt_unix_free_job(&job->job);
    RAISE(rd_errno, "Failed to consume messages (%s)", rd_kafka_err2str(rd_errno));
  }

  caml_remove_generational_global_root(&(job->caml_kafka_queue));
  lwt_unix_free_job(&job->job);
  CAMLreturn(caml_msg_list);
}

/* The stub creating the [consume_batch_queue] job structure. */
extern CAMLprim
value ocaml_kafka_consume_batch_queue_job(value caml_kafka_queue, value caml_kafka_timeout, value caml_msg_count)
{
  CAMLparam3(caml_kafka_queue, caml_kafka_timeout, caml_msg_count);
  int msg_count = Int_val(caml_msg_count);
  if (msg_count < 0) msg_count = 0;

  struct job_consume_batch_queue* job = (struct job_consume_batch_queue*)
    lwt_unix_new_plus(struct job_consume_batch_queue, msg_count * sizeof (rd_kafka_message_t*));

  job->caml_kafka_queue = caml_kafka_queue;
  caml_register_generational_global_root(&(job->caml_kafka_queue));
  job->queue = get_handler(Field(caml_kafka_queue,0));
  job->timeout = Int_val(caml_kafka_timeout);
  job->msg_count = msg_count;

  job->job.worker = (lwt_unix_job_worker)worker_consume_batch_queue;
  job->job.result = (lwt_unix_job_result)result_consume_batch_queue;

  CAMLreturn(lwt_unix_alloc_job(&job->job));
}
