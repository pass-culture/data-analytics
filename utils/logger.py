import logging
import os
from logging import INFO as LOG_LEVEL_INFO

LOG_LEVEL = int(os.environ.get("LOG_LEVEL", LOG_LEVEL_INFO))


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=LOG_LEVEL,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def pc_logging(level, *args):
    global logging
    if logging.getLogger().isEnabledFor(level):
        evaled_args = map(lambda a: a() if callable(a) else a, args)
        logging.log(level, *evaled_args)


logger = AttrDict()
logger.critical = lambda *args: pc_logging(logging.CRITICAL, *args)
logger.debug = lambda *args: pc_logging(logging.DEBUG, *args)
logger.error = lambda *args: pc_logging(logging.ERROR, *args)
logger.info = lambda *args: pc_logging(logging.INFO, *args)
logger.warning = lambda *args: pc_logging(logging.WARNING, *args)
