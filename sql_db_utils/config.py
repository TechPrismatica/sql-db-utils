import pathlib
from typing import Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class _ModuleConfig(BaseSettings):
    MODULE_NAME: str
    DEFER_GEN_REFRESH: bool = Field(default=False)


class _PostgresConfig(BaseSettings):
    POSTGRES_URI: str
    PG_MIN_CONNECTION: int = Field(default=1)
    PG_MAX_CONNECTION: int = Field(default=10)
    PG_CONNECTION_TIMEOUT: int = Field(default=30)
    PG_ANTI_PERSISTENT: bool = Field(default=False)
    PG_MAX_RETRY: int = Field(default=5)
    PG_RETRY_QUERY: bool = Field(default=False)
    PG_POOL_RECYCLE: int = Field(default=300)
    PG_CONNECT_ARGS: dict = Field(
        default={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        }
    )
    PG_ENABLE_POOLING: bool = Field(default=False)
    PG_DEFER_POLARS: bool = Field(default=False)
    PG_DEFAULT_SCHEMA: str = Field(default="public")

    @field_validator("POSTGRES_URI", mode="before")
    def validate_my_field(cls, value):
        value = value.strip("/")
        import urllib.parse

        unquoted_postgres_uri = urllib.parse.unquote(value)
        value = urllib.parse.quote(unquoted_postgres_uri, safe=":/@")
        value = value.replace("postgresql://", "postgresql+psycopg://")
        return value


class _RedisConfig(BaseSettings):
    REDIS_URI: str
    REDIS_PROJECT_TAGS_DB: int = 18


class _BasePathConf(BaseSettings):
    BASE_PATH: str = "/code/data"


class _PathConf(BaseSettings):
    BASE_PATH: pathlib.Path = pathlib.Path(_BasePathConf().BASE_PATH)
    DECLARATIVES_PATH: pathlib.Path = BASE_PATH / "sql_declaratives"

    @model_validator(mode="after")
    def validate_paths(self) -> Self:
        if not self.DECLARATIVES_PATH.exists():
            self.DECLARATIVES_PATH.mkdir(parents=True)
        return self


ModuleConfig = _ModuleConfig()
PostgresConfig = _PostgresConfig()
RedisConfig = _RedisConfig()
PathConfig = _PathConf()

__all__ = ["ModuleConfig", "PostgresConfig", "RedisConfig", "PathConfig"]
