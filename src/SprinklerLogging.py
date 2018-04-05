import logging.config
import json, os


# Creates the log directory if it does not exist
def create_log_dir():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    log_dir = os.path.join(dir_path, '..', 'logs')
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)


# Configures application wide logging from JSON file
def configure_logging():
    create_log_dir()
    with open('logging.json', 'r') as log_file:
        log_dict = json.load(log_file)
        logging.config.dictConfig(log_dict)
        logger = logging.getLogger(__name__)
        logger.info("Logging has been configured")
