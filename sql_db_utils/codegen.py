from textwrap import indent
from typing import Sequence, override

from sqlacodegen.generators import Base, DeclarativeGenerator, LiteralImport
from sqlacodegen.models import ModelClass
from sqlalchemy import MetaData
from sqlalchemy.engine import Connection, Engine

from sql_db_utils.config import PostgresConfig


class UTDeclarativeGenerator(DeclarativeGenerator):
    @override
    def __init__(
        self,
        raw_database: str,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str],
        *,
        indentation: str = "    ",
        schema: str = PostgresConfig.PG_DEFAULT_SCHEMA,
        base_class_name: str = "Base",
    ):
        super().__init__(metadata, bind, options, indentation=indentation, base_class_name=base_class_name)
        self.raw_database = raw_database
        self.schema = schema

    @override
    def generate_base(self) -> None:
        self.base = Base(
            literal_imports=[LiteralImport("sql_db_utils.declaratives", "DeclarativeBaseClassFactory")],
            declarations=[f'{self.base_class_name} = DeclarativeBaseClassFactory("{self.raw_database}")'],
            metadata_ref=f"{self.base_class_name}.metadata",
        )

    @override
    def render_class_declaration(self, model: ModelClass) -> str:
        parent_class_name = model.parent_class.name if model.parent_class else self.base_class_name
        base_text = f"class {model.name}({parent_class_name}):"
        return "\n".join([base_text, indent('__table_args__ = {"extend_existing": True}', self.indentation)])
