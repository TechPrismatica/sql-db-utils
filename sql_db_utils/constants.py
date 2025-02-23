from enum import StrEnum

ENUMPARENT = (StrEnum,)


class QueryType(*ENUMPARENT):
    """
    An enumeration representing the different types of queries that can be executed.

    Attributes:
        JSON (str): The query result will be returned as a JSON object.
        PANDAS (str): The query result will be returned as a Pandas DataFrame.
        POLAR (str): The query result will be returned as a Polars DataFrame.
    """

    JSON = "json"
    PANDAS = "pandas"
    POLAR = "polars"


class AGGridDateTrim(*ENUMPARENT):
    """
    An enumeration representing the different date trimming options

    Attributes:
        YEAR (str): Trims the date to the year
        MONTH (str): Trims the date to the month
        DAY (str): Trims the date to the day
        HOUR (str): Trims the date to the hour
        MINUTE (str): Trims the date to the minute
        SECOND (str): Trims the date to the second
        MILLISECOND (str): Trims the date to the millisecond
    """

    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    MILLISECOND = "millisecond"
