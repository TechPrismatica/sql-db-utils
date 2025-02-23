import logging
import sys

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine


async def create_default_table_executor(_engine: AsyncEngine, metadata: MetaData):
    """
    Creates default tables in the database using the provided SQLAlchemy engine and metadata.

    Args:
        _engine (Engine): SQLAlchemy engine object.
        metadata (MetaData): SQLAlchemy metadata object.

    Raises:
        Exception: If an error occurs while creating the tables.

    Returns:
        None
    """
    try:
        async with _engine.begin() as conn:
            await conn.run_sync(metadata.create_all, checkfirst=True)
    except Exception as e:
        logging.error(f"Error occurred while creating: {e}", exc_info=True)
        sys.exit()


async def create_default_psql_dependencies(metadata: MetaData, engine_obj: AsyncEngine):
    """
    Creates default PostgreSQL dependencies.

    Args:
        metadata (MetaData): The metadata object containing the table definitions.
        engine_obj (Engine, optional): The SQLAlchemy engine object to use. Defaults to None.

    Raises:
        Exception: If an error occurs while creating the tables.

    """
    try:
        await create_default_table_executor(engine_obj, metadata)
    except Exception as e:
        logging.error(f"Error occurred while creating: {e}", exc_info=True)
        raise e
