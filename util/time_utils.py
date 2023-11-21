from datetime import datetime, timedelta
from time import time
from typing import List, Tuple


def str_to_datetime(date_str: str) -> datetime:
    """
    Convert a date string in DD/MM/YYYY format to a datetime object.

    :param date_str: Date string in the format DD/MM/YYYY.
    :return: datetime object representing the given date.
    :raises ValueError: If the date string is not in the correct format.
    """
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except ValueError as e:
        raise ValueError("Incorrect data format, should be DD/MM/YYYY") from e


def determine_period(start_date: datetime) -> int:
    """
    Calculate the number of days from the given start_date to the current date.

    :param start_date: datetime object representing the start date
    :return: Number of days from start_date to today
    """
    return (datetime.today().date() - start_date.date()).days


def determine_days_interval(days_interval: int, period: int) -> int:
    """
    Determine the smaller of two day intervals.

    If args_interval is provided and truthy, compare it with period and
    return the smaller one. If args_interval is falsy (e.g., None or 0),
    default to 30 days.

    :param args_interval: User-provided interval in days or a falsy value.
    :param period: A pre-defined period in days to compare against.
    :return: The smaller of args_interval (or 30 if falsy) and period.
    """
    return min(days_interval or 30, period)


def determine_timestamp_now() -> int:
    """
    Get the current time as a timestamp in milliseconds.

    :return: Current time as a timestamp in milliseconds.
    """
    return int(time() * 1000)


def determine_timestamp_start_time(end_time: int, days_interval: int) -> int:
    """
    Calculate the start time as a timestamp in milliseconds, given the end time
    and a days interval.

    :param end_time: End time as a timestamp in milliseconds.
    :param days_interval: Number of days as the interval.
    :return: Start time as a timestamp in milliseconds.
    """
    # Convert end_time from milliseconds to seconds for datetime
    end_time_datetime = datetime.fromtimestamp(end_time / 1000)
    interval_time = timedelta(days=days_interval)

    # Convert back to milliseconds after computing the start time
    return int((end_time_datetime - interval_time).timestamp() * 1000)


def determine_start_end_times(
    period: int, days_interval: int = 1
) -> List[Tuple[int, int]]:
    end_time = determine_timestamp_now()
    start_time = determine_timestamp_start_time(end_time, days_interval)
    times = [(start_time, end_time)]

    while period > 0:
        end_time = start_time
        start_time = determine_timestamp_start_time(end_time, days_interval)

        times.append((start_time, end_time))

        period -= days_interval
        days_interval = determine_days_interval(days_interval, period)

    return times
