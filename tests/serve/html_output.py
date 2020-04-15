from dataflows import Flow
import logging


class MyClass():
    pass


def flow(*_):
    logging.info('my_object=' + str(MyClass()))
    return Flow()
