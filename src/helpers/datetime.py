from datetime import date
from typing import Any, Union


def convert_possible_date_to_str(obj: Any) -> Union[str, None]:
    """This is to provide a hack for serialising date objects to JSON."""
    if isinstance(obj, date):
        return obj.strftime("%Y%m%d")
    else:
        return obj
