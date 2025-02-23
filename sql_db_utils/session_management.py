import logging
from typing import Callable, Union

from redis import Redis
from sqlalchemy import Engine, MetaData, NullPool, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy_utils import create_database, database_exists

from sql_db_utils.config import ModuleConfig, PostgresConfig
from sql_db_utils.declaratives import DeclarativeBaseClassFactory
from sql_db_utils.sql_creations import create_default_psql_dependencies
from sql_db_utils.sql_retry_handler import RetryingQuery


class SQLSessionManager:
    def __init__(self, redis_project_db: Union[Redis, None] = None, database_uri: Union[str, None] = None) -> None:
        self._db_engines = {}
        if not redis_project_db:
            from sql_db_utils.redis_connections import project_db as redis_project_db
        self.redis_project_source_db = redis_project_db
        self.database_uri = database_uri or PostgresConfig.POSTGRES_URI

    def _get_fully_qualified_db(self, database: str, project_id: Union[str, None] = None) -> str:
        return f"{project_id}__{database}" if project_id else database

    def _ensure_engine_connection(self, _engine_obj: Engine):
        for _ in range(PostgresConfig.PG_MAX_RETRY):
            try:
                if not database_exists(_engine_obj.url):
                    create_database(_engine_obj.url)
                with _engine_obj.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    break
            except OperationalError as oe:
                if "server login has been failing" not in str(oe):
                    logging.info(f"Server connection failed, retry {_}")
                    continue
                logging.error("Server connection failed")

    def _get_engine(
        self, database: str, project_id: Union[str, None] = None, metadata: Union[MetaData, None] = None
    ) -> Engine:
        qualified_db_name = self._get_fully_qualified_db(database=database, project_id=project_id)
        if not (engine := self._db_engines.get(qualified_db_name)):
            logging.debug(f"Creating engine for database: {qualified_db_name}")
            if PostgresConfig.PG_ENABLE_POOLING:
                engine = create_engine(
                    f"{self.database_uri}/{qualified_db_name}?application_name={ModuleConfig.MODULE_NAME}",
                    connect_args=(
                        {
                            "connect_timeout": PostgresConfig.PG_CONNECTION_TIMEOUT,
                        }
                        | PostgresConfig.PG_CONNECT_ARGS
                    ),
                    pool_size=PostgresConfig.PG_MIN_CONNECTION,
                    max_overflow=PostgresConfig.PG_MAX_CONNECTION,
                    pool_pre_ping=True,
                    pool_use_lifo=True,
                    future=True,
                    pool_recycle=PostgresConfig.PG_POOL_RECYCLE,
                    isolation_level="AUTOCOMMIT",
                )
            else:
                engine = create_engine(
                    f"{self.database_uri}/{qualified_db_name}?application_name={ModuleConfig.MODULE_NAME}",
                    connect_args=(
                        {
                            "connect_timeout": PostgresConfig.PG_CONNECTION_TIMEOUT,
                        }
                        | PostgresConfig.PG_CONNECT_ARGS
                    ),
                    poolclass=NullPool,
                    future=True,
                    isolation_level="AUTOCOMMIT",
                )
            self._ensure_engine_connection(engine)
            if not PostgresConfig.PG_ANTI_PERSISTENT:
                self._db_engines[qualified_db_name] = engine
            create_default_psql_dependencies(
                metadata=metadata or DeclarativeBaseClassFactory(database).metadata, engine_obj=engine
            )
        return engine

    def get_session(
        self,
        database: str,
        project_id: Union[str, None] = None,
        metadata: Union[MetaData, None] = None,
        retrying: bool = False,
    ) -> Session:
        if PostgresConfig.PG_RETRY_QUERY or retrying:
            return Session(
                bind=self._get_engine(database=database, project_id=project_id, metadata=metadata),
                future=True,
                query_cls=RetryingQuery,
            )
        return Session(
            bind=self._get_engine(database=database, project_id=project_id, metadata=metadata),
            future=True,
        )

    def get_engine_obj(
        self, database: str, project_id: Union[str, None] = None, metadata: Union[MetaData, None] = None
    ) -> Engine:
        return self._get_engine(database=database, project_id=project_id, metadata=metadata)

    def get_db_factory(self, database: str, security_enabled: bool = True, retrying: bool = False) -> Callable:
        if security_enabled:
            try:
                from ut_security_util import MetaInfoSchema

                def get_db(meta: MetaInfoSchema):
                    yield self.get_session(database=database, project_id=meta.project_id, retrying=retrying)

                return get_db
            except ImportError:
                logging.error("ut_security_util not installed, please install it to use security features")
                raise
        else:
            from fastapi import Request

            async def get_db(request: Request):
                cookies = request.cookies
                project_id = cookies.get("project_id")
                yield self.get_session(database=database, project_id=project_id, retrying=retrying)

            return get_db
