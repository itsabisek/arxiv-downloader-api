import logging


def bootstrap_logger(name, logfile='error.log'):
    formatter = logging.Formatter(
        "%(asctime)s %(filename)s (%(funcName)s, %(lineno)d) [%(levelname)s] - %(message)s")
    handler = logging.FileHandler(f'logs/{logfile}')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
