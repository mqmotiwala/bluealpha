from dateutil import parser as dateutil_parser


def parse_date(value):
    """Parse a date string in any common format and return ISO YYYY-MM-DD.

    Handles: YYYY-MM-DD, MM/DD/YYYY, MM-DD-YYYY, YYYY/MM/DD,
    DD-Mon-YYYY, 'January 4, 2024', and other formats supported by dateutil.

    Returns the date as a string in YYYY-MM-DD format.
    Raises ValueError if the string cannot be parsed.
    """
    dt = dateutil_parser.parse(value)
    return dt.strftime("%Y-%m-%d")
