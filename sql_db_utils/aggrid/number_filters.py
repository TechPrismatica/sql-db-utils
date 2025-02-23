from typing import Any

from sqlalchemy import null


class AGGridNumberFilters:
    def __init__(self, mappings: dict) -> None:
        self.mappings = mappings

    def __call__(self, filter_type: str, filter: Any, column: str) -> bool:
        match filter_type:
            case "equal" | "equals":
                return self._equal(filter, column)
            case "doesNotEqual" | "notEqual":
                return self._not_equal(filter, column)
            case "greaterThan":
                return self._greater_than(filter, column)
            case "greaterThanOrEqualTo":
                return self._greater_than_or_equal_to(filter, column)
            case "lessThan":
                return self._less_than(filter, column)
            case "lessThanOrEqualTo":
                return self._less_than_or_equal_to(filter, column)
            case "between":
                return self._between(filter, column)
            case "blank":
                return self._blank(column)
            case "notBlank":
                return self._not_blank(column)
            case "inRange":
                return self._in_range(filter, column)
            case _:
                return False

    def _equal(self, filter: Any, column: str) -> bool:
        return self.mappings[column] == filter

    def _greater_than(self, filter: Any, column: str) -> bool:
        return self.mappings[column] > filter

    def _greater_than_or_equal_to(self, filter: Any, column: str) -> bool:
        return self.mappings[column] >= filter

    def _less_than(self, filter: Any, column: str) -> bool:
        return self.mappings[column] < filter

    def _less_than_or_equal_to(self, filter: Any, column: str) -> bool:
        return self.mappings[column] <= filter

    def _between(self, filter: tuple, column: str) -> bool:
        lower, upper = filter
        return lower <= self.mappings[column] <= upper

    def _blank(self, column: str) -> bool:
        return self.mappings[column].is_(null())

    def _not_blank(self, column: str) -> bool:
        return self.mappings[column].is_not(null())

    def _not_equal(self, filter: Any, column: str) -> bool:
        return self.mappings[column] != filter

    def _in_range(self, filter: tuple, column: str) -> bool:
        return self._between(filter, column)
