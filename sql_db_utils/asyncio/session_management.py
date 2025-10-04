import logging
from typing import Annotated, Any, AsyncGenerator, Callable, List, Union

from sqlalchemy import Engine, MetaData, NullPool, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from sql_db_utils.asyncio.declaratives import DeclarativeBaseClassFactory
from sql_db_utils.asyncio.sql_creation_helper import create_database, database_exists
from sql_db_utils.asyncio.sql_creations import create_default_psql_dependencies
from sql_db_utils.asyncio.sql_retry_handler import RetryingQuery
from sql_db_utils.config import ModuleConfig, PostgresConfig


class SQLSessionManager:
    __slots__ = (
        "_db_engines",
        "database_uri",
        "_postcreate_auto",
        "_postcreate_manual",
        "_precreate_auto",
        "_precreate_manual",
    )

    def __init__(self, database_uri: Union[str, None] = None) -> None:
        self._db_engines = {}
        self.database_uri = database_uri or PostgresConfig.POSTGRES_URI
        self._postcreate_auto: dict = {}
        self._postcreate_manual: dict = {}
        self._precreate_auto: dict = {}
        self._precreate_manual: dict = {}

    def __del__(self) -> None:
        for engine in self._db_engines.values():
            engine.dispose()

    def _get_fully_qualified_db(self, database: str, tenant_id: Union[str, None] = None) -> str:
        return f"{tenant_id}__{database}" if tenant_id else database

    async def _ensure_engine_connection(self, _engine_obj: Engine):
        for _ in range(PostgresConfig.PG_MAX_RETRY):
            try:
                if not await database_exists(_engine_obj.url):
                    await create_database(_engine_obj.url)
                async with _engine_obj.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                    break
            except OperationalError as oe:
                if "server login has been failing" not in str(oe):
                    logging.info(f"Server connection failed, retry {_}")
                    continue
                logging.error("Server connection failed")

    async def _get_engine(
        self, database: str, tenant_id: Union[str, None] = None, metadata: Union[MetaData, None] = None
    ) -> AsyncSession:
        qualified_db_name = self._get_fully_qualified_db(database=database, tenant_id=tenant_id)
        if not (engine := self._db_engines.get(qualified_db_name)):
            logging.debug(f"Creating engine for database: {qualified_db_name}")
            if PostgresConfig.PG_ENABLE_POOLING:
                engine = create_async_engine(
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
                engine = create_async_engine(
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
            await self._ensure_engine_connection(engine)
            if not PostgresConfig.PG_ANTI_PERSISTENT:
                self._db_engines[qualified_db_name] = engine
            await self.run_precreate(engine, database, tenant_id)
            await create_default_psql_dependencies(
                metadata=metadata or DeclarativeBaseClassFactory(database).metadata, engine_obj=engine
            )
            await self.run_postcreate(engine, database, tenant_id)
        return engine

    async def get_session(
        self,
        database: str,
        tenant_id: Union[str, None] = None,
        metadata: Union[MetaData, None] = None,
        retrying: bool = False,
    ) -> AsyncSession:
        if PostgresConfig.PG_RETRY_QUERY or retrying:
            return AsyncSession(
                bind=self._get_engine(database=database, tenant_id=tenant_id, metadata=metadata),
                future=True,
                query_cls=RetryingQuery,
            )
        return AsyncSession(
            bind=await self._get_engine(database=database, tenant_id=tenant_id, metadata=metadata),
            expire_on_commit=False,
            future=True,
        )

    async def get_engine_obj(
        self, database: str, tenant_id: Union[str, None] = None, metadata: Union[MetaData, None] = None
    ) -> AsyncEngine:
        return await self._get_engine(database=database, tenant_id=tenant_id, metadata=metadata)

    def get_db_factory(self, database: str, retrying: bool = False) -> AsyncGenerator[AsyncSession, Any]:
        from fastapi import Cookie

        async def get_db(tenant_id: Annotated[str, Cookie()] = None) -> AsyncGenerator[AsyncSession, Any]:
            yield await self.get_session(database=database, tenant_id=tenant_id, retrying=retrying)

        return get_db

    def postcreate_decorator(self, raw_db: str | List[str], postcreate_store: str) -> Callable:
        postcreate_store = getattr(self, postcreate_store)

        def decorator(func: Callable) -> None:
            if isinstance(raw_db, list):
                for db in raw_db:
                    postcreate_auto = postcreate_store.get(db, [])
                    postcreate_auto.append(func)
                    postcreate_store[db] = postcreate_auto
            else:
                postcreate_auto = postcreate_store.get(raw_db, [])
                postcreate_auto.append(func)
                postcreate_store[raw_db] = postcreate_auto

        return decorator

    def precreate_decorator(self, raw_db: str | List[str], precreate_store: str) -> Callable:
        precreate_store = getattr(self, precreate_store)

        def decorator(func: Callable) -> None:
            if isinstance(raw_db, list):
                for db in raw_db:
                    precreate_auto = precreate_store.get(db, [])
                    precreate_auto.append(func)
                    precreate_store[db] = precreate_auto
            else:
                precreate_auto = precreate_store.get(raw_db, [])
                precreate_auto.append(func)
                precreate_store[raw_db] = precreate_auto

        return decorator

    def register_postcreate(self, raw_db: str | List[str]) -> Callable:
        return self.postcreate_decorator(raw_db, "_postcreate_auto")

    def register_postcreate_manual(self, raw_db: str | List[str]) -> Callable:
        return self.postcreate_decorator(raw_db, "_postcreate_manual")

    def register_precreate(self, raw_db: str | List[str]) -> Callable:
        return self.precreate_decorator(raw_db, "_precreate_auto")

    def register_precreate_manual(self, raw_db: str | List[str]) -> Callable:
        return self.precreate_decorator(raw_db, "_precreate_manual")

    async def run_precreate(self, engine: AsyncEngine, raw_db: str, tenant_id: Union[str, None] = None) -> None:
        session = AsyncSession(bind=engine, future=True, expire_on_commit=False)
        async with session.begin():
            for precreate_func in self._precreate_auto.get(raw_db, []):
                result = precreate_func(tenant_id)
                if isinstance(result, list):
                    for statement in result:
                        await session.execute(statement)
                else:
                    await session.execute(result)
        for precreate_func in self._precreate_manual.get(raw_db, []):
            await precreate_func(session, tenant_id)
        await session.commit()
        await session.close()
        logging.info(f"Precreate for {raw_db} completed")

    async def run_postcreate(self, engine: AsyncEngine, raw_db: str, tenant_id: Union[str, None] = None) -> None:
        session = AsyncSession(bind=engine, future=True, expire_on_commit=False)
        async with session.begin():
            for postcreate_func in self._postcreate_auto.get(raw_db, []):
                result = postcreate_func(tenant_id)
                if isinstance(result, list):
                    for statement in result:
                        await session.execute(statement)
                else:
                    await session.execute(result)
        for postcreate_func in self._postcreate_manual.get(raw_db, []):
            await postcreate_func(session, tenant_id)
        await session.commit()
        await session.close()
        logging.info(f"Postcreate for {raw_db} completed")
