import timeout
from json import dumps, loads
from pikachu.cuda_utils import is_oom_cuda_error, is_cuda_error
from pikachu.client import AMQPClient
from pikachu.basic_consume import SAFE_MESSAGE_TIMEOUT

SAFE_MESSAGE_TIMEOUT = (
    29 * 60
)  # 29 minutes because broker's delivery timeout is 30 minutes by default, -1 for safety


@timeout.timeout(duration=SAFE_MESSAGE_TIMEOUT)
def handle_message(
    client,
    consumed_message,
    method,
    message_function,
    logger,
    request_id_name="request_id",
):
    method, _properties, message_txt = consumed_message
    message_json = loads(message_txt)
    request_id = message_json[request_id_name]
    logger.info(f"[*] Received {request_id_name}: {request_id}.")
    try:
        result = message_function(message_json)
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


def start(logger, message_function, request_id_name="request_id"):
    client = AMQPClient.from_config()

    for consumed_message in client.consume():
        handle_message(
            client,
            consumed_message,
            message_function,
            logger,
            request_id_name=request_id_name,
        )

    client.teardown()
