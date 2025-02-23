from sqlalchemy import func, null

from sql_db_utils.constants import AGGridDateTrim
from sql_db_utils.datetime_utils import parse


class AGGridDateFilters:
    def __init__(self, mappings: dict, date_trim: AGGridDateTrim = None, tz: str = None) -> None:
        self.mappings = mappings
        self.date_trim = date_trim or AGGridDateTrim.DAY
        self.tz = tz or "UTC"

    def __call__(self, filter_type: str, date_from: str, column: str, date_to: str = None) -> bool:
        match filter_type:
            case "equals":
                return self._equals(date_from, column)
            case "doesNotEqual" | "notEqual":
                return self._does_not_equal(date_from, column)
            case "before" | "lessThan":
                return self._before(date_from, column)
            case "after" | "greaterThan":
                return self._after(date_from, column)
            case "between" | "inRange":
                return self._between(date_from, date_to, column)
            case "blank":
                return self._blank(column)
            case "notBlank":
                return self._not_blank(column)
            case "greaterThanOrEqualTo":
                return self._greater_than_or_equal_to(date_from, column)
            case "lessThanOrEqualTo":
                return self._less_than_or_equal_to(date_from, column)
            case _:
                return False

    def _equals(self, date_from: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]) == parse(date_from, tz=self.tz)

    def _does_not_equal(self, date_from: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]) != parse(date_from, tz=self.tz)

    def _before(self, date_from: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]) < parse(date_from, tz=self.tz)

    def _after(self, date_from: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]) > parse(date_from, tz=self.tz)

    def _between(self, date_from: str, date_to: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]).between(
            parse(date_from, tz=self.tz), parse(date_to, tz=self.tz)
        )

    def _blank(self, column: str) -> bool:
        return self.mappings[column].is_(null())

    def _not_blank(self, column: str) -> bool:
        return self.mappings[column].is_not(null())

    def _greater_than_or_equal_to(self, date_from: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]) >= parse(date_from, tz=self.tz)

    def _less_than_or_equal_to(self, date_from: str, column: str) -> bool:
        return func.date_trunc(self.date_trim.value, self.mappings[column]) <= parse(date_from, tz=self.tz)
