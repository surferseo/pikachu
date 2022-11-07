from json import dumps, loads
from pikachu.cuda_utils import is_oom_cuda_error, is_cuda_error
from pikachu.client import AMQPClient
import traceback

# TODO handle broker timeout (possibly without spawning a new process)


def handle_message(
    client,
    consumed_message,
    message_function,
    models=None,
    request_id_name="request_id",
    logger=None,
):
    method, _properties, message_txt = consumed_message
    delivery_tag = method.delivery_tag
    message_json = loads(message_txt)
    request_id = message_json[request_id_name]
    logger.info(f"[*] Received {request_id_name}: {request_id}.")
    try:
        result_dict = message_function(message_json, models)
        logger.info(f"[*] Done request id: {request_id}.")
        result_dict.update({request_id_name: request_id})
        client.publish_and_ack(delivery_tag, dumps(result_dict))
    except Exception as e:
        if is_cuda_error(e) and not is_oom_cuda_error(e):
            # if something is wrong with CUDA, further consuming is pointless
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
            client.publish(result)


def start(message_function, models=None, request_id_name="request_id", logger=None):
    client = AMQPClient.from_config()

    for consumed_message in client.consume():
        handle_message(
            client,
            consumed_message,
            message_function,
            models=models,
            request_id_name=request_id_name,
            logger=logger,
        )

    client.teardown()
