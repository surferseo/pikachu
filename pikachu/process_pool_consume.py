import functools
from json import dumps, loads
import torch.multiprocessing as mp
from .client import Client as MessagingClient
from pebble import ProcessPool
from pikachu.client import AMQPClient

DEFAULT_BROKER_TIMEOUT = 29 * 60  # -1 minute for safety
MAINTENANCE_TIMEOUT = 5 * 60
MAINTENANCE_MESSAGE = (None, None, None)


def on_task_done(
    client, delivery_tag, redelivered, message_json, message_txt, request_id, future
):
    try:
        response_message = future.result()
        LOGGER.info(f"[*] Done brief for request_id: {request_id}.")
        client.publish_and_ack(delivery_tag, dumps(response_message), message_txt)
    except Exception as e:
        if is_cuda_error(e) and not is_oom_cuda_error(e):
            # if something is wrong with CUDA, further consuming is pointless
            raise e
        elif not redelivered:
            LOGGER.error(
                f"[*] Brief for request {request_id} failed. Redelivering message.",
                exc_info=True,
            )
            client.reject(delivery_tag)
        else:
            LOGGER.error(
                f"[*] Brief for redelivered request {request_id} failed. Publishing empty result to response queue and redirecting message to failures queue. ",
                exc_info=True,
            )
            result = dumps(serialize_result(message_json, request_id, []))
            client.publish_and_ack(delivery_tag, result, message_txt, failed=True)


def parse_message(message_txt, delivery_tag, client):
    request_id = None
    message_json = None
    try:
        message_json = loads(message_txt)
        request_id = message_json.get("brief_id") or message_json.get(
            "domain_planner_page_id"
        )
        if not request_id:
            raise Exception(
                "Missing tracking id in message. Provide one of the following: brief_id, domain_planner_page_id."
            )
        LOGGER.info(f"[*] Brief query request_id: {request_id}.")
    except:
        LOGGER.error(
            f"[*] Cannot parse message. Publishing empty result to response queue and redirecting message to failures queue.",
            exc_info=True,
        )
        result = dumps(serialize_result(message_json, "unknown", []))
        client.publish_and_ack(delivery_tag, result, message_txt, failed=True)
    return request_id, message_json


def propagate_callback_cuda_exceptions(futures):
    LOGGER.info(
        "Futures: "
        + str(len(futures))
        + str([(f.running(), f.cancelled(), f.done()) for f in futures]),
    )
    done_futures = [f for f in futures if f.done()]
    for f in done_futures:
        e = f.exception()
        if e:
            LOGGER.error(f"Exception in callback:\n{str(e)}")
            if is_cuda_error(e) and not is_oom_cuda_error(e):
                raise e
    for f in done_futures:
        futures.remove(f)


def start(
    logger,
    message_function,
    message_args,
    request_id_name="request_id",
    n_processes=1,
    maintenance_timeout=MAINTENANCE_TIMEOUT,
):
    client = AMQPClient.from_config()
    mp_context = mp.get_context("spawn")

    with ProcessPool(
        n_processes,
        context=mp_context,
    ) as pool:
        futures = []
        for consumed_message in client.consume(maintenance_timeout):
            propagate_callback_cuda_exceptions(futures)

            if consumed_message == MAINTENANCE_MESSAGE:
                continue

            method, _properties, message_txt = consumed_message
            request_id, message_json = parse_message(
                message_txt, method.delivery_tag, client
            )
            if request_id is not None:
                future = pool.schedule(
                    message_function,
                    message_args,
                    timeout=DEFAULT_BROKER_TIMEOUT,
                )
                task_done_callback = functools.partial(
                    on_task_done,
                    client,
                    method.delivery_tag,
                    method.redelivered,
                    message_json,
                    message_txt,
                    request_id,
                )
                future.add_done_callback(task_done_callback)

    client.teardown()
