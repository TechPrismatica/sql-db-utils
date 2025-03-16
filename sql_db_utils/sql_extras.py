from typing import Any, List, Optional

from sqlalchemy.ext import compiler
from sqlalchemy.schema import CreateColumn, DDLElement


class CreateExtension(DDLElement):
    def __init__(self, name: str):
        self.name = name


@compiler.compiles(CreateExtension)
def compile_create_extension(element: CreateExtension, _compiler: Any, **__kwargs__) -> str:
    return f"CREATE EXTENSION IF NOT EXISTS {element.name};"


class CreateServer(DDLElement):
    def __init__(self, server_name: str, remote_db_name: str, remote_host: str, remote_port: int):
        self.server_name = server_name
        self.remote_db_name = remote_db_name
        self.remote_host = remote_host
        self.remote_port = remote_port


@compiler.compiles(CreateServer)
def compile_create_server(element: CreateServer, _compiler: Any, **__kwargs__) -> str:
    return f"""
        CREATE SERVER IF NOT EXISTS {element.server_name}
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
        dbname '{element.remote_db_name}',
        host '{element.remote_host}',
        port '{element.remote_port}'
        );
    """


class DropServer(DDLElement):
    def __init__(self, server_name: str):
        self.server_name = server_name


@compiler.compiles(DropServer)
def compile_drop_server(element: DropServer, _compiler: Any, **__kwargs__) -> str:
    return f"DROP SERVER IF EXISTS {element.server_name} CASCADE;"


class CreateUserMapping(DDLElement):
    def __init__(self, role: str, server_name: str, remote_role: str, remote_password: str):
        self.role = role
        self.server_name = server_name
        self.remote_role = remote_role
        self.remote_password = remote_password


@compiler.compiles(CreateUserMapping)
def compile_create_user_mapping(element: CreateUserMapping, compiler: Any, **kw: Any) -> str:
    return f"""
        CREATE USER MAPPING FOR {element.role}
        SERVER {element.server_name}
        OPTIONS (user '{element.remote_role}', password '{element.remote_password}');
    """


class DropUserMapping(DDLElement):
    def __init__(self, role: str, server_name: str):
        self.role = role
        self.server_name = server_name


@compiler.compiles(DropUserMapping)
def compile_drop_user_mapping(element: DropUserMapping, compiler: Any, **kw: Any) -> str:
    return f"DROP USER MAPPING IF EXISTS FOR {element.role} SERVER {element.server_name};"


class CreateForeignTable(DDLElement):
    def __init__(
        self,
        table_name: str,
        columns: List[Any],
        server_name: str,
        remote_schema_name: str,
        remote_table_name: str,
        local_schema_name: Optional[str] = None,
    ):
        self.local_schema_name = local_schema_name or "public"
        self.table_name = table_name
        self.columns = columns
        self.server_name = server_name
        self.remote_schema_name = remote_schema_name
        self.remote_table_name = remote_table_name


@compiler.compiles(CreateForeignTable)
def compile_create_foreign_table(element: CreateForeignTable, compiler: Any, **kw: Any) -> str:
    columns = [compiler.process(CreateColumn(column), **kw) for column in element.columns]
    return f"""
        CREATE FOREIGN TABLE {element.local_schema_name}.{element.table_name}
        ({", ".join(columns)})
        SERVER {element.server_name}
        OPTIONS(schema_name '{element.remote_schema_name}', table_name '{element.remote_table_name}');
    """


class DropForeignTable(DDLElement):
    def __init__(self, name: str):
        self.name = name


@compiler.compiles(DropForeignTable)
def compile_drop_foreign_table(element: DropForeignTable, compiler: Any, **kw: Any) -> str:
    return f"DROP FOREIGN TABLE IF EXISTS {element.name};"


class CreatePrefixedIdFunction(DDLElement):
    def __init__(self, function_name: str):
        self.function_name = function_name


@compiler.compiles(CreatePrefixedIdFunction)
def compile_create_prefixed_id_function(element: CreatePrefixedIdFunction, _compiler: Any, **__kwargs__) -> str:
    return f"""
        CREATE OR REPLACE FUNCTION {element.function_name}(prefix TEXT, seq_name TEXT)
        RETURNS TEXT
        AS $$
        DECLARE
          next_val INTEGER;
        BEGIN
          next_val := nextval(seq_name);
          RETURN prefix || next_val;
        END;
        $$ LANGUAGE plpgsql;
    """


class CreateSuffixedIdFunction(DDLElement):
    def __init__(self, function_name: str):
        self.function_name = function_name


@compiler.compiles(CreateSuffixedIdFunction)
def compile_create_suffixed_id_function(element: CreateSuffixedIdFunction, _compiler: Any, **__kwargs__) -> str:
    return f"""
        CREATE OR REPLACE FUNCTION {element.function_name}(seq_name TEXT, suffix TEXT)
        RETURNS TEXT
        AS $$
        DECLARE
          next_val INTEGER;
        BEGIN
          next_val := nextval(seq_name);
          RETURN next_val || suffix;
        END;
        $$ LANGUAGE plpgsql;
    """
