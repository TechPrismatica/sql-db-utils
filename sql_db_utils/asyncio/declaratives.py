import datetime

from sqlalchemy import TIMESTAMP
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase

from sql_db_utils.config import PostgresConfig

base_classes = None
DeclarativeBaseClassFactory = None


class BaseClasses:
    """
    Generates a base class for all different databases.
    """

    def __init__(self) -> None:
        global base_classes
        base_classes = {}

    class DeclarativeBaseClassFactory:
        """
        Base class factory to maintain a single base class for each database.
        """

        def __new__(
            cls,
            raw_database: str,
            schema: str = PostgresConfig.PG_DEFAULT_SCHEMA,
            custom_type_annotations: dict = None,
            disable_timestamp: bool = False,
        ) -> DeclarativeBase:
            global base_classes
            if not (Base := base_classes.get(f"{raw_database}_{schema}")):  # NOSONAR

                class Base(AsyncAttrs, DeclarativeBase):
                    """
                    Base class for all database models.
                    """

                    global base_classes

                    type_annotation_map = (
                        custom_type_annotations or {datetime.datetime: TIMESTAMP(timezone=True)}
                        if not disable_timestamp
                        else {}
                    )

                base_classes[f"{raw_database}_{schema}"] = Base
                return Base
            return Base

    def remove_base_class(self, raw_database: str, schema: str = PostgresConfig.PG_DEFAULT_SCHEMA) -> None:
        global base_classes
        if base_classes.get(f"{raw_database}_{schema}"):
            del base_classes[f"{raw_database}_{schema}"]


base_class_generator = BaseClasses()
DeclarativeBaseClassFactory = base_class_generator.DeclarativeBaseClassFactory

__all__ = ["DeclarativeBaseClassFactory"]
