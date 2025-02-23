from typing import Any, Callable, List, Tuple

from sqlalchemy import and_, any_, or_
from sqlalchemy.orm import Mapped
from whenever import Instant

from sql_db_utils.aggrid.date_filters import AGGridDateFilters
from sql_db_utils.aggrid.number_filters import AGGridNumberFilters
from sql_db_utils.aggrid.text_filters import AGGridTextFilters
from sql_db_utils.constants import AGGridDateTrim


class AGGridUtils:
    def __init__(
        self,
        aggrid_column_mappings: dict[str, Mapped],
        aggrid_filters: dict,
        date_trim: AGGridDateTrim = AGGridDateTrim.DAY,
        aggrid_column_options: dict = None,
        tz: str = None,
    ) -> None:
        self.mappings = aggrid_column_mappings
        self.options = aggrid_column_options or {}
        self.sorters = aggrid_filters.get("sortModel", [])
        self.filters = aggrid_filters.get("filterModel", {})
        self.text_filters = AGGridTextFilters(self.mappings)
        self.number_filters = AGGridNumberFilters(self.mappings)
        self.date_filters = AGGridDateFilters(self.mappings, date_trim, tz)

    def __call__(self) -> Tuple:
        return self._get_filter(), self._get_sorter()

    def _filter_condition_operator(self, operator) -> Callable:
        if operator == "AND":
            return and_
        return or_

    def _sort_value(self, column: str, direction: str) -> Any:
        return self.mappings[column].asc() if direction == "asc" else self.mappings[column].desc()

    def _get_sorter(self) -> List:
        return [self._sort_value(sorter["colId"], sorter["sort"]) for sorter in self.sorters]

    def _type_filter(self, filter_column, filters) -> bool:
        filter_type = filters.get("filterType")
        if filter_type == "text":
            return self.text_filters(filters["type"], filters["filter"], filter_column)
        elif filter_type == "number":
            return self.number_filters(filters["type"], filters["filter"], filter_column)
        elif filter_type == "date":
            return self.date_filters(filters["type"], filters["dateFrom"], filter_column, filters.get("dateTo"))

    def _get_filter(self) -> List:
        where_conditions = []
        for filter_column, filters in self.filters.items():
            filter_kwargs = self.options.get(filter_column, {})
            if exec_op := filters.get("operator"):
                condition_merger = []
                for k, v in filters.items():
                    if k.startswith("condition"):
                        condition_merger.append(self._type_filter(filter_column, v))
                where_conditions.append(self._filter_condition_operator(exec_op)(*condition_merger))
            elif list_filter := filters.get("values"):
                where_conditions.append(self._selection_filter(list_filter, filter_column, **filter_kwargs))
            elif filters.get("filterType"):
                where_conditions.append(self._type_filter(filter_column, filters))
            elif date_filter := filters.get("filter"):
                where_conditions.append(
                    self.date_filters(
                        "inRange",
                        Instant.from_timestamp(date_filter[0] // 1000).format_common_iso(),
                        filter_column,
                        Instant.from_timestamp(date_filter[1] // 1000).format_common_iso(),
                    )
                )
        return where_conditions

    def _selection_filter(self, values: List, column: str, ilike: bool = False) -> bool:
        if ilike:
            return self.mappings[column].ilike(any_(values))
        return self.mappings[column].in_(values)
