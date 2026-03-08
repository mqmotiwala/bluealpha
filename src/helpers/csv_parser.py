"""Shared CSV parser with comma-in-field repair.

Wraps csv.DictReader and detects rows where an unquoted comma inside a field
value (e.g. "January 4, 2024") caused the row to split into one extra column.
When detected, the parser attempts to rejoin consecutive field pairs that form
a valid date, then shifts the remaining fields back into alignment.
"""

import csv
import io
import logging

from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)


def _try_parse_date(value):
    """Return True if value is parseable as a date, False otherwise."""
    try:
        dateutil_parser.parse(value)
        return True
    except (ValueError, OverflowError):
        return False


def _repair_row(row, headers):
    """Attempt to repair a row that has one extra column due to a comma-split field.

    Scans consecutive header pairs, tries rejoining their values with a comma,
    and checks if the result parses as a date. If so, rejoins that field and
    shifts everything after it back into place.

    Returns the repaired row dict, or None if repair failed.
    """
    overflow_values = row.get(None)
    if not overflow_values or len(overflow_values) != 1:
        return None

    overflow_value = overflow_values[0]

    for i in range(len(headers) - 1):
        left_header = headers[i]
        right_header = headers[i + 1]
        rejoined = row[left_header] + ", " + row[right_header]

        if _try_parse_date(rejoined):
            repaired = {}
            for j, header in enumerate(headers):
                if j < i:
                    repaired[header] = row[header]
                elif j == i:
                    repaired[header] = rejoined
                elif j < len(headers) - 1:
                    # Shift left: this header gets the value of the next header
                    repaired[header] = row[headers[j + 1]]
                else:
                    # Last header gets the overflow value
                    repaired[header] = overflow_value

            logger.info(
                "Repaired comma-split date in row: rejoined '%s' for field '%s'",
                rejoined, left_header,
            )
            return repaired

    return None


def parse_csv(raw_body):
    """Parse raw CSV bytes/string into a list of row dicts, repairing comma-split fields.

    Any row where csv.DictReader produces an overflow column (None key) is
    passed through the repair algorithm. If repair fails, the row is included
    as-is (downstream Pydantic validation will catch it).
    """
    text = raw_body.decode("utf-8") if isinstance(raw_body, bytes) else raw_body
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames
    rows = []

    for row in reader:
        if None in row:
            repaired = _repair_row(row, headers)
            if repaired is not None:
                rows.append(repaired)
            else:
                logger.warning(
                    "Row has extra columns but repair failed — passing through as-is: %s",
                    {k: v for k, v in row.items() if k is not None},
                )
                # Remove the None key so downstream code doesn't choke on it
                row.pop(None, None)
                rows.append(row)
        else:
            rows.append(row)

    return rows
