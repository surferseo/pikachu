from json import dumps, loads
from pikachu.cuda_utils import is_oom_cuda_error, is_cuda_error
from pikachu.client import AMQPClient

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
    message_json = loads(message_txt)
    request_id = message_json[request_id_name]
    logger.info(f"[*] Received {request_id_name}: {request_id}.")
    try:
        result = message_function(message_json, models)
        logger.info(f"[*] Done request id: {request_id}.")
        client.publish_and_ack(
            method.delivery_tag, dumps({request_id_name: request_id, "result": result})
        )
    except Exception as e:
        if is_cuda_error(e) and not is_oom_cuda_error(e):
            # if something is wrong with CUDA, further consuming is pointless
            raise e
        logger.error(
            f"[*] Failed {request_id_name}: {request_id}. Publishing empty result to the response queue.",
            exc_info=True,
        )
        result = dumps({request_id_name: request_id, "result": []})
        client.publish_and_ack(method.delivery_tag, result)


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
