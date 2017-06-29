#!/usr/bin/env python3

import logging
import logging.config
import os

from tornado.ioloop import IOLoop
from tornado.web import Application

import web_handlers
from engine import Engine


def default_logging(log_level):
    int_level = logging._nameToLevel[log_level]
    return {
        'version': 1,
        'formatters': {
            'main_formatter': {'format': '%(asctime)s %(levelname)s %(name)s: %(message)s'},
        },
        'handlers': {
            'console': {'class': 'logging.StreamHandler', 'formatter': 'main_formatter'},
        },
        'loggers': {
            'tornado.application': {'level': logging.ERROR},
            'tornado.access': {'level': (int_level + 10) if int_level < 50 else 50},
            'engine': {},
            'stand': {},
        },
        'root': {
            'level': int_level,
            'handlers': ['console']
        },
    }


def main():
    application = Application([
        (r'/', web_handlers.MainPageHandler),
        (r'/s/([a-z,0-9,\-,_]+)/*([a-z]*)', web_handlers.ContainerActions),
        (r'/admin/*', web_handlers.AdminPageHandler),
        (r'/(.*)', web_handlers.MassActionHandler),

    ])

    params = {
        'domain_name': '172.17.0.1',
        'image': 'tandemservice/test-tools',
        'max_active_stands': 6,
        'stop_timeout': '480',
        'log_level': 'INFO'
    }

    for key, val in params.items():
        if key in os.environ:
            params[key] = os.environ[key]

    if params['domain_name'] in ('localhost', '127.0.0.1'):
        raise RuntimeError('Do not use localhost cause docker host is not localhost for container. '
                           'Usually ip of eth0 is correct')

    logging.config.dictConfig(default_logging(params.pop('log_level')))
    application.engine = Engine(**params)

    application.listen(8888)
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
