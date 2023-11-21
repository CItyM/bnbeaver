from hashlib import sha256
from typing import Iterable, Set

from model import Transaction


def hash_values(values: Iterable) -> bytes:
    """
    Generate a SHA-256 hash of the provided values.

    This function takes a variable number of arguments, concatenates
    them using a pipe (|) as a delimiter after converting each to a string,
    and returns the SHA-256 hash of the resulting concatenated string.

    Parameters:
    values: List of values to be hashed. Non-string values will be converted
            to strings.

    Returns:
    bytes: The SHA-256 hash of the concatenated values.

    Example:
    >>> generate_hash("John", 123, 45.6)
    b'\x1fO\xa4\x8b\xe7\xd1\xf3\xa0\xe0\xd6...\xe2\x8c\x98\xd8\x8c\x0e\x92\x8f'
    """

    values = list(values)
    values = [str(value) for value in values]
    concatenated_data = "|".join(values)
    return sha256(concatenated_data.encode()).digest()


def hash_transactions(transactions: Iterable[Transaction]) -> Set[bytes]:
    """
    Generate a set of hashes for each row in the input.

    :param rows: An iterable of rows, where each row is a dictionary.
    :return: A set of unique hash values for the rows.
    """
    return set(map(lambda tx: hash_values(tx.__dict__.values()), transactions))
