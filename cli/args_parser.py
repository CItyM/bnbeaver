from typing import Tuple
from util import determine_period, determine_days_interval, str_to_datetime
from argparse import ArgumentParser


def args_parser() -> Tuple[int, int] | None:
    parser = ArgumentParser()

    parser.add_argument(
        "-d",
        "--date",
        type=str,
        help="The date from which to start tracking transaction (DD/MM/YYYY)",
    )

    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        help=(
            """
            The interval in days of the first and last transaction
            (min: 1; max: 30;)
            """
        ),
    )

    args = parser.parse_args()

    if args.date:
        start_date = str_to_datetime(args.date)
        period = determine_period(start_date)
        days_interval = determine_days_interval(args.interval, period)
        return (period, days_interval)
