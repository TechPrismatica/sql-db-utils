from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncSession

from sql_db_utils.config import PostgresConfig


class InspectorUtils:
    def __init__(self, session: AsyncSession, schema: str = None) -> None:
        self.session = session
        self.schema = schema

    async def __call__(
        self, function: str, schema: str = PostgresConfig.PG_DEFAULT_SCHEMA, table_name: str = None, **kwargs
    ):
        schema = self.schema or schema
        async with self.session.bind.begin() as conn:
            return await conn.run_sync(self._executor, table_name, function, schema, kwargs)

    def _executor(self, conn, table_name, function, schema, kwargs):
        inspector = inspect(conn)
        exec_func = getattr(inspector, function)
        if table_name:
            return exec_func(table_name, schema=schema, **kwargs)
        return exec_func(schema=schema, **kwargs)
