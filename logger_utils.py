import logging


def bootstrap_logger(name):
    formatter = logging.Formatter(
        "%(asctime)s %(name)s [%(levelname)s] - %(message)s")
    handler = logging.FileHandler('error.log')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
