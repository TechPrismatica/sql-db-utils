import logging as logger
from typing import Generic, List, Tuple, TypeVar, Union

from fastapi.encoders import jsonable_encoder
from sqlalchemy import Select, Table, delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

from sql_db_utils.aggrid import AGGridUtils
from sql_db_utils.config import PostgresConfig
from sql_db_utils.constants import QueryType

TableType = TypeVar("TableType", bound=Union[DeclarativeBase, Table])


class SqlAlchemyUtil(Generic[TableType]):
    """
    A utility class for performing SQL operations using SQLAlchemy V2.
    """

    def __init__(self, session: Session, table: TableType = None):
        """
        Initializes a new instance of the SqlAlchemyUtil class.

        Args:
            session (Session): The SQLAlchemy session object.
            table (TableType, optional): The SQLAlchemy declarative base object. Defaults to None.
        """
        self.session = session
        self.table = table

    def __del__(self):
        """
        Closes the SQLAlchemy session.
        """
        logger.debug("Closing SQL session!")
        self.session.close()

    def insert(self, data: Union[dict, list[dict]], return_keys: List[str] = None, table: TableType = None):
        """
        Inserts a single row into the database.

        Args:
            data (dict): A dictionary containing the data to be inserted.
            return_keys (List[str], optional): A list of column names to return after the insert. Defaults to None.
            table (TableType, optional): The SQLAlchemy declarative base object. Defaults to None.

        Returns:
            A list of dictionaries containing the inserted data.
        """
        table = table if table is not None else self.table
        return_keys = return_keys or []
        try:
            insert_stmt = insert(table).values(data).returning(*(getattr(table.c, key) for key in return_keys))
            return_values = self.session.execute(insert_stmt)
            self.session.commit()
            if return_keys:
                return jsonable_encoder(return_values.mappings().all())
        except Exception as e:
            logger.error(f"Error occurred while inserting: {e}", exc_info=True)
            raise e

    def update_with_where(
        self,
        data: Union[dict, list[dict]],
        where_conditions: List,
        return_keys: List[str] = None,
        table: TableType = None,
    ):
        """
        Updates rows in the database based on the given conditions.

        Args:
            data (dict): A dictionary containing the data to be updated.
            where_conditions (List): A list of conditions to filter the data.
            table (TableType, optional): The SQLAlchemy declarative base object. Defaults to None.
        """
        table = table if table is not None else self.table
        return_keys = return_keys or []
        try:
            update_stmt = (
                update(table)
                .values(data)
                .where(*where_conditions)
                .returning(*(getattr(table.c, key) for key in return_keys))
            )
            return_values = self.session.execute(update_stmt)
            self.session.commit()
            if return_keys:
                return jsonable_encoder(return_values.mappings().all())
        except Exception as e:
            logger.error(f"Error occurred while updating: {e}", exc_info=True)
            raise e

    def update(self, data: Union[dict, list[dict]], return_keys: List[str] = None, table: TableType = None):
        """
        Updates multiple rows in the database.

        Args:
            data (List[dict]): A list of dictionaries containing the data to be updated.
            table (TableType, optional): The SQLAlchemy declarative base object. Defaults to None.
        """
        table = table if table is not None else self.table
        return_keys = return_keys or []
        try:
            return_values = self.session.execute(
                update(table).returning(*(getattr(table.c, key) for key in return_keys)), data
            )
            self.session.commit()
            if return_keys:
                return jsonable_encoder(return_values.mappings().all())
        except Exception as e:
            logger.error(f"Error occurred while updating: {e}", exc_info=True)
            raise e

    def upsert(self, insert_json: dict, primary_keys: List[str] = None, table: TableType = None):
        """
        Inserts or updates a row in the database.

        Args:
            insert_json (dict): A dictionary containing the data to be inserted or updated.
            primary_keys (List[str], optional): A list of primary key column names. Defaults to None.
            table (TableType, optional): The SQLAlchemy declarative base object. Defaults to None.
        """
        table = table if table is not None else self.table
        try:
            insert_statement = (
                postgres_insert(table)
                .values(**insert_json)
                .on_conflict_do_update(index_elements=primary_keys, set_=insert_json)
            )
            self.session.execute(insert_statement)
            self.session.commit()
        except Exception as e:
            logger.error(f"Error while upserting the record {e}", exc_info=True)
            raise e

    def delete(self, where_conditions: List, return_keys: List[str] = None, table: TableType = None):
        """
        Deletes rows from the database based on the given conditions.

        Args:
            where_conditions (List): A list of conditions to filter the data.
            table (TableType, optional): The SQLAlchemy declarative base object. Defaults to None.
        """
        table = table if table is not None else self.table
        return_keys = return_keys or []
        try:
            delete_stmt = (
                delete(table)
                .where(*where_conditions)
                .returning(*(getattr(table.c if isinstance(table, Table) else table, key) for key in return_keys))
            )
            return_values = self.session.execute(delete_stmt)
            self.session.commit()
            if return_keys:
                return jsonable_encoder(return_values.mappings().all())
        except Exception as e:
            logger.error(f"Error occurred while deleting: {e}", exc_info=True)
            raise e

    def _get_columns(self, columns: List, table: TableType) -> List:
        columns_updated = []
        for column in columns:
            if isinstance(column, str):
                columns_updated.append(getattr(table, column))
            elif isinstance(column, DeclarativeAttributeIntercept):
                columns_updated.extend(list(column.__table__.columns))
            else:
                columns_updated.append(column)
        return columns_updated

    def _build_select_query(
        self,
        table: TableType,
        where_conditions: List,
        offset: int = None,
        columns: Tuple[str] = None,
        order_by: List = None,
        group_by: List = None,
    ):
        """
        Builds a select query based on the given conditions.

        Args:
            table (TableType): The SQLAlchemy declarative base object.
            where_conditions (List): A list of conditions to filter the data.
            columns (Tuple[str], optional): A tuple of column names to select. Defaults to None.

        Returns:
            The built select query.
        """
        order_by = order_by or []
        group_by = group_by or []
        select_stmt = select(*(table.columns.values() if isinstance(table, Table) else table.__table__.columns))
        if columns:
            select_stmt = select_stmt.with_only_columns(*self._get_columns(columns, table))
        return select_stmt.where(*where_conditions).order_by(*order_by).group_by(*group_by).offset(offset)

    def _get_count(self, table: TableType, where_conditions: List):
        """
        Returns the count of rows in the given table.

        Args:
            table (TableType): The SQLAlchemy declarative base object.
            where_conditions (List): A list of conditions to filter the data.

        Returns:
            The count of rows in the given table.
        """
        count_query = select(table).with_only_columns(func.count()).order_by(None).where(*where_conditions)
        return self.session.execute(count_query).scalar()

    def _get_count_advanced(self, select_stmt: Select):
        """
        Returns the count of rows in the given table.

        Args:
            select_stmt (Select): The SQLAlchemy select object.

        Returns:
            The count of rows in the given table.
        """
        count_query = select(func.count()).select_from(select_stmt.subquery())
        return self.session.execute(count_query).scalar()

    def select_from_table(
        self,  # NOSONAR
        where_conditions: List,
        columns: Tuple[str] = None,
        select_one: bool = False,
        offset: int = None,
        limit: int = None,
        return_count: bool = False,
        return_type: QueryType = QueryType.JSON,
        query_kwargs: dict = None,
        order_by: List = None,
        group_by: List = None,
        table: TableType = None,
        aggrid_filters: dict = None,
        aggrid_column_mappings: dict = None,
        aggrid_column_options: dict = None,
        aggrid_options: dict = None,
    ):
        """
        Selects data from a table based on the given conditions.

        Args:
            where_conditions (List): A list of conditions to filter the data by.
            columns (Tuple[str], optional): A tuple of column names to select. Defaults to None.
            select_one (bool, optional): Whether to select only one row. Defaults to False.
            offset (int, optional): The number of rows to skip. Defaults to None.
            limit (int, optional): The maximum number of rows to return. Defaults to None.
            return_count (bool, optional): Whether to return the count of rows. Defaults to False.
            return_type (QueryType, optional): The type of query to return. Defaults to QueryType.JSON.
            table (TableType, optional): The table to select from. Defaults to None.

        Returns:
            The selected data.
        """
        table = table if table is not None else self.table
        order_by = order_by or []
        group_by = group_by or []
        aggrid_column_mappings = aggrid_column_mappings or {}
        aggrid_filters = aggrid_filters or {}
        query_kwargs = query_kwargs or {}
        aggrid_options = aggrid_options or {}
        try:
            aggrid_util = AGGridUtils(
                aggrid_column_mappings,
                aggrid_filters,
                aggrid_options.pop("date_trim", None),
                aggrid_column_options,
                aggrid_options.pop("tz", None),
            )
            ag_filters, ag_sorters = aggrid_util()
            where_conditions.extend(ag_filters)
            ag_sorters.extend(order_by)
            order_by = ag_sorters
            select_stmt = self._build_select_query(table, where_conditions, offset, columns, order_by, group_by)
            if select_one:
                return jsonable_encoder(self.session.execute(select_stmt).mappings().first())
            results = self.fetch_by_query(select_stmt.limit(limit), return_type, **query_kwargs)
            if return_count:
                return (self._get_count(table, where_conditions), results)
            return results
        except Exception as e:
            logger.error(f"Error occurred while fetching: {e}", exc_info=True)
            raise e

    def _prepare_joins(self, select_stmt, joins, join_additional_where_conditions):
        for join in joins:
            if isinstance(join, tuple):
                select_stmt = select_stmt.join(*join)
            elif isinstance(join, dict):
                select_stmt = select_stmt.join(**join)
        if join_additional_where_conditions:
            select_stmt = select_stmt.where(*join_additional_where_conditions)
        return select_stmt

    def select_from_table_advanced(
        self,  # NOSONAR noqa
        where_conditions: List,
        columns: Tuple[str] = None,
        select_stmt: Select = None,
        select_one: bool = False,
        offset: int = None,
        limit: int = None,
        return_count: bool = False,
        return_type: QueryType = QueryType.JSON,
        query_kwargs: dict = None,
        order_by: List = None,
        group_by: List = None,
        aggrid_filters: dict = None,
        aggrid_column_mappings: dict = None,
        aggrid_options: dict = None,
        aggrid_column_options: dict = None,
        table: TableType = None,
        joins: List = None,
        join_additional_where_conditions: List = None,
    ):
        table = table if table is not None else self.table
        order_by = order_by or []
        group_by = group_by or []
        aggrid_column_mappings = aggrid_column_mappings or {}
        aggrid_filters = aggrid_filters or {}
        query_kwargs = query_kwargs or {}
        aggrid_options = aggrid_options or {}
        try:
            aggrid_util = AGGridUtils(
                aggrid_column_mappings,
                aggrid_filters,
                aggrid_options.pop("date_trim", None),
                aggrid_column_options,
                aggrid_options.pop("tz", None),
            )
            ag_filters, ag_sorters = aggrid_util()
            where_conditions.extend(ag_filters)
            if select_stmt is None:
                select_stmt = self._build_select_query(table, where_conditions, offset, columns, order_by, group_by)
            else:
                select_stmt = select_stmt.where(*where_conditions)
            if joins:
                select_stmt = self._prepare_joins(select_stmt, joins, join_additional_where_conditions)
            base_stmt = select_stmt.order_by(*ag_sorters, *order_by).group_by(*group_by)
            select_stmt = base_stmt.offset(offset)
            if select_one:
                return jsonable_encoder(self.session.execute(select_stmt).mappings().first())
            results = self.fetch_by_query(select_stmt.limit(limit), return_type, **query_kwargs)
            if return_count:
                return (self._get_count_advanced(base_stmt), results)
            return results
        except Exception as e:
            logger.error(f"Error occurred while fetching: {e}", exc_info=True)
            raise e

    def fetch_as_polars(self, query, stream: bool = False, chunksize: int = None, **__kwargs__):
        """
        Fetches data from the database using Polars library.

        Args:
            query: SQLAlchemy query object.

        Returns:
            A Polars DataFrame object containing the fetched data.

        Raises:
            ImportError: If Polars library is not installed.
            Exception: If an error occurs while fetching the data.
        """
        try:
            pl = None
            try:
                import polars as pl
            except ImportError as ie:
                logger.debug("Polars not installed, Failed to fetch using polars")
                raise ie
            return pl.read_database(
                query,
                connection=self.session.bind,
                execute_options={"parameters": query.compile().params},
                iter_batches=stream,
                **{"batch_size": chunksize} if stream else {},
            )
        except Exception as e:
            logger.error(f"Error occurred while fetching using polars: {e}")

    def fetch_as_pandas(self, query, stream: bool = False, chunksize: int = None, arrow: bool = False, **__kwargs__):
        """
        Fetches data from the database using pandas or polars library.

        Args:
            query (str): SQL query to execute.

        Returns:
            pandas.DataFrame: DataFrame containing the results of the query.

        Raises:
            ImportError: If neither pandas nor polars library is installed.
            Exception: If an error occurs while fetching data using pandas or polars.
        """
        try:
            if not PostgresConfig.PG_DEFER_POLARS:
                try:
                    import polars as pl

                    if stream:
                        return pl.read_database(
                            query,
                            connection=self.session.bind,
                            execute_options={"parameters": query.compile().params},
                            iter_batches=stream,
                            **{"batch_size": chunksize} if stream else {},
                        )

                    polars_df = pl.read_database(query, connection=self.session.bind)
                    return polars_df.to_pandas(use_pyarrow_extension_array=arrow)
                except ImportError:
                    logger.debug("Polars not installed, Using Fall back to pandas")
            try:
                import pandas as pd

                if stream:
                    return pd.read_sql(
                        query,
                        self.session.bind.execution_options(stream_results=True, max_row_buffer=chunksize),
                        chunksize=chunksize,
                        **({"dtype_backend": "pyarrow"} if arrow else {}),
                    )
                return pd.read_sql(
                    query,
                    self.session.bind,
                    **({"dtype_backend": "pyarrow"} if arrow else {}),
                )
            except ImportError as ie:
                logger.error("Pandas not installed, Failed to fetch using pandas")
                raise ie
        except Exception as e:
            logger.error(f"Error occurred while fetching using pandas: {e}")

    def fetch_as_json(self, query, **__kwargs__):
        """
        Executes the given SQL query and returns the result as a list of dictionaries.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: A list of dictionaries representing the result of the query.

        Raises:
            Exception: If an error occurs while fetching data.
        """
        try:
            return jsonable_encoder(self.session.execute(query).mappings().all())
        except Exception as e:
            logger.error(f"Error occurred while fetching data: {e}")

    def fetch_by_query(self, query, query_type: QueryType = QueryType.JSON, **__kwargs__):
        """
        Fetches data from the database using the provided query and returns the result in the specified format.

        Args:
            query (str): The SQL query to execute.
            query_type (QueryType, optional): The format in which to return the result. Defaults to QueryType.JSON.

        Returns:
            The result of the query in the specified format.
        """
        try:
            callable_func = getattr(self, f"fetch_as_{query_type.value}")
            return callable_func(query, **__kwargs__)
        except Exception as e:
            logger.error(f"Error occurred while fetching: {e}")
