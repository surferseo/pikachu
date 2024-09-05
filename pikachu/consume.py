from json import dumps, loads
import pika
from pikachu.client import AMQPClient
from pikachu.cuda_errors import is_unknown_cuda_error
import traceback
from pebble import ProcessPool, ProcessFuture
import functools
import importlib
from retry import retry

DEFAULT_BROKER_TIMEOUT = (
    29 * 60
)  # 30m is Rabbit's broker default ack timeout, -1 minute to handle it here before broker closes connection
MAINTENANCE_TIMEOUT = 5 * 60
MAINTENANCE_MESSAGE = (None, None, None)


@retry(pika.exceptions.AMQPConnectionError, delay=5)
def start(
    message_function,
    models=None,
    request_id_name="request_id",
    logger=None,
    timeout=DEFAULT_BROKER_TIMEOUT,
    use_torch_cuda=False,
):
    client = AMQPClient.from_config()
    prefetch_count = client.get_prefetch_count()

    if prefetch_count == 1:
        _start_consuming_single_message(
            client, message_function, models, request_id_name, logger
        )
    else:
        _start_consuming_multiple_messages(
            client,
            message_function,
            models,
            request_id_name,
            logger,
            prefetch_count,
            use_torch_cuda,
            timeout,
        )

    client.teardown()


def _start_consuming_single_message(
    client, message_function, models, request_id_name, logger
):
    for message in client.consume(None):
        method, properties, message_txt = message
        message_json = loads(message_txt)
        request_id = message_json[request_id_name]
        logger.info(f"[*] Received {request_id_name}: {request_id}.")
        result = message_function(message_json, models)
        _handle_result(client, message, request_id_name, logger, result)


def _start_consuming_multiple_messages(
    client,
    message_function,
    models,
    request_id_name,
    logger,
    prefetch_count,
    use_torch_cuda,
    timeout,
):
    mp_context = _get_multiprocessing_context(use_torch_cuda)
    with ProcessPool(
        prefetch_count,
        context=mp_context,
    ) as pool:
        for message in client.consume(MAINTENANCE_TIMEOUT):
            if use_torch_cuda:
                _propagate_callback_cuda_exceptions(pool)

            if message == MAINTENANCE_MESSAGE:
                continue

            method, properties, message_txt = message
            message_json = loads(message_txt)
            request_id = message_json[request_id_name]
            logger.info(f"[*] Received {request_id_name}: {request_id}.")

            future = pool.schedule(
                message_function,
                (message_json, models),
                timeout=timeout,
            )
            done_callback = functools.partial(
                _handle_result,
                client,
                message,
                request_id_name,
                logger,
            )
            future.add_done_callback(done_callback)


def _get_multiprocessing_context(use_torch_cuda):
    try:
        module_name = "torch.multiprocessing" if use_torch_cuda else "multiprocessing"
        mp = importlib.import_module(module_name)
        return mp.get_context("spawn")
    except ImportError:
        return None


def _propagate_callback_cuda_exceptions(pool):
    """
    This is a workaround for https://github.com/NVIDIA/nvidia-docker/issues/1671.
    We look for CUDA exceptions in the consumer processes and raise them here.
    """
    pool._check_pool_state()

    futures = [task.future for task in pool._context.task_queue.queue]
    done_futures = [f for f in futures if f.done()]
    for f in done_futures:
        e = f.exception()
        if e and is_unknown_cuda_error(e):
            raise e


def _handle_result(
    client,
    consumed_message,
    request_id_name,
    logger,
    result,
):
    method, properties, message_txt = consumed_message
    delivery_tag = method.delivery_tag
    message_json = loads(message_txt)
    request_id = message_json[request_id_name]
    try:
        if isinstance(result, ProcessFuture):
            result = result.result()
        logger.info(f"[*] Done request id: {request_id}.")
        result.update({request_id_name: request_id})
        client.publish_and_ack(delivery_tag, properties, dumps(result))
    except Exception as e:
        if is_unknown_cuda_error(e):
            # if something is wrong with CUDA, further consuming is pointless
            #
            # when prefetch_count == 1 and there's no pool of processes, we raise the error to kill the app
            #
            # when prefetch_count > 1 and we have a pool of processes
            # we find this exception from consumer process and raise to restart the app
            # we can't successfuly raise it from here to restart the app, because it is ignored by design:
            # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.add_done_callback
            raise e
        elif not method.redelivered:
            logger.error(
                f"[*] Failed {request_id_name}: {request_id}. Redelivering.",
                exc_info=True,
            )
            client.reject(delivery_tag, requeue=True)
        else:
            logger.error(
                f"[*] Failed {request_id_name}: {request_id}. Publishing error message to the response queue.",
                exc_info=True,
            )
            result = dumps(
                {request_id_name: request_id, "error": traceback.format_exc()}
            )
            client.reject(delivery_tag, requeue=False)
            client.publish(properties, result)
