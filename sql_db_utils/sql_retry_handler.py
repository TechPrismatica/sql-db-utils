import logging

from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy.orm.query import Query as _Query


class RetryingQuery(_Query):
    __max_retry_count__ = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __iter__(self):
        attempts = 0
        while True:
            attempts += 1
            try:
                return super().__iter__()
            except OperationalError as ex:
                if "server closed the connection unexpectedly" not in str(ex):
                    raise
                if attempts <= self.__max_retry_count__:
                    sleep_for = 2 ** (attempts - 1)
                    logging.error(
                        f"/!\\ Database connection error: retrying Strategy =>"
                        f"sleeping for {sleep_for}s and will retry "
                        f"(attempt #{attempts} of {self.__max_retry_count__}) \n"
                        f"Detailed query impacted: {ex}"
                    )
                    continue
                else:
                    raise
            except StatementError as ex:
                if "reconnect until invalid transaction is rolled back" not in str(ex):
                    raise
                self.session.rollback()
