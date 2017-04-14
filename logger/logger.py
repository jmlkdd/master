import os
import datetime
import logging
from logging.handlers import RotatingFileHandler


def get_logger(task_name=None, log_dir=None, level=logging.DEBUG, console_out=True):
    if log_dir is None:
        log_dir = os.path.expandvars('$HOME') + '/var/logs/'
    elif log_dir[0] != '/':
        log_dir = os.path.dirname(os.path.abspath(__file__)) + '/' + log_dir + '/'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if task_name is None:
        task_name = 'root'
    log_file = log_dir + "%s.logger" % task_name

    if isinstance(level, basestring):
        level = level.lower()
    if level == "debug":
        level = logging.DEBUG
    elif level == "info":
        level = logging.INFO
    elif level == "warning":
        level = logging.WARNING
    elif level == "error":
        level = logging.ERROR
    else:
        level = logging.INFO
    logger = logging.getLogger('root')
    fmt = "%(asctime)s - %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)
    handler = RotatingFileHandler(log_file, maxBytes=64 * 1024 * 1025, backupCount=5)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if console_out is True:
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    logger.setLevel(level)
    return logger

# logger = get_logger()
# logger.info('reefege')
