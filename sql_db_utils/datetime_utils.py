import contextlib
import copy
import datetime
from typing import Any

from dateutil import parser
from whenever import Instant, ZonedDateTime

DEFAULT_OPTIONS = {
    "day_first": True,
    "year_first": False,
    "strict": False,
    "exact": False,
    "now": None,
}


def parse(text: str, **_options: Any) -> datetime.datetime:
    options = copy.copy(DEFAULT_OPTIONS)
    options.update(_options)

    with contextlib.suppress(ValueError):
        return Instant.parse_common_iso(text).py_datetime()
    with contextlib.suppress(ValueError):
        return ZonedDateTime.parse_common_iso(text).py_datetime()
    with contextlib.suppress(ValueError):
        return Instant.parse_rfc2822(text).py_datetime()
    with contextlib.suppress(ValueError):
        return Instant.parse_rfc3339(text).py_datetime()

    if options.get("strict"):
        raise ValueError("Invalid datetime string")
    else:
        try:
            dt = parser.parse(text, dayfirst=options["day_first"], yearfirst=options["year_first"])
        except ValueError:
            raise ValueError("Invalid datetime string")
    return dt
