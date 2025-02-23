from typing import Any


class AGGridTextFilters:
    def __init__(self, mappings: dict) -> None:
        self.mappings = mappings

    def __call__(self, filter_type: str, filter: Any, column: str) -> bool:
        match filter_type:
            case "contains":
                return self._contains(filter, column)
            case "notContains":
                return self._not_contains(filter, column)
            case "equals":
                return self._equals(filter, column)
            case "notEqual":
                return self._not_equal(filter, column)
            case "startsWith":
                return self._starts_with(filter, column)
            case "endsWith":
                return self._ends_with(filter, column)
            case "blank":
                return self._blank(column)
            case "notBlank":
                return self._not_blank(column)
            case _:
                return False

    def _contains(self, filter: Any, column: str) -> bool:
        return self.mappings[column].ilike(f"%{filter}%")

    def _not_contains(self, filter: Any, column: str) -> bool:
        return self.mappings[column].not_ilike(f"%{filter}%")

    def _equals(self, filter: Any, column: str) -> bool:
        return self.mappings[column] == filter

    def _not_equal(self, filter: Any, column: str) -> bool:
        return self.mappings[column] != filter

    def _starts_with(self, filter: Any, column: str) -> bool:
        return self.mappings[column].ilike(f"{filter}%")

    def _ends_with(self, filter: Any, column: str) -> bool:
        return self.mappings[column].ilike(f"%{filter}")

    def _blank(self, column: str) -> bool:
        return self.mappings[column] == None  # noqa NOSONAR

    def _not_blank(self, column: str) -> bool:
        return self.mappings[column] != None  # noqa NOSONAR
