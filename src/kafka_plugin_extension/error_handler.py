from abc import ABC, abstractmethod


class ErrorHandler(ABC):
    def __init__(self, logger, log_error=True):
        super().__init__()
        self.log_errors = log_error
        self.log = logger

    @abstractmethod
    def handle_error(self, error, msg=None):
        # msg will need for consumer in case if it needs to reproduce to Kafka topic back
        raise NotImplemented('Please implement error handler first')

    def log_error(self, message, error):
        self.log.exception(f"ERROR: {message}")
        self.log.exception(f"Exception: {error}")
