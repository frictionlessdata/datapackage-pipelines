import logging
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.exceptions import RequestException

tpe = ThreadPoolExecutor(max_workers=1)


def _send(hook, payload):
    try:
        response = requests.post(hook, json=payload)
        if response.status_code != 200:
            logging.warning('Server returned %s, hook %s with payload %r ',
                            response.status_code, hook, payload)
    except RequestException as e:
        logging.warning('Failed to call hook %s with payload %r (%s)',
                        hook, payload, e)


class HookSender():
    def send(self, hook, payload, blocking=False):
        if blocking:
            _send(hook, payload)
        else:
            tpe.submit(_send, hook, payload)


hook_sender = HookSender()
