import logging
import sys

from sqlalchemy import Engine, MetaData


def create_default_table_executor(_engine: Engine, metadata: MetaData):
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
        metadata.create_all(_engine, checkfirst=True)
    except Exception as e:
        logging.error(f"Error occurred while creating: {e}", exc_info=True)
        sys.exit()


def create_default_psql_dependencies(metadata: MetaData, engine_obj: Engine):
    """
    Creates default PostgreSQL dependencies.

    Args:
        metadata (MetaData): The metadata object containing the table definitions.
        engine_obj (Engine, optional): The SQLAlchemy engine object to use. Defaults to None.

    Raises:
        Exception: If an error occurs while creating the tables.

    """
    try:
        create_default_table_executor(engine_obj, metadata)
    except Exception as e:
        logging.error(f"Error occurred while creating: {e}", exc_info=True)
        raise e
