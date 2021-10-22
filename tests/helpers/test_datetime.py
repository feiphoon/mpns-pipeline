from datetime import date
import pytest
from src.helpers.datetime import convert_possible_date_to_str


class TestConvertPossibleDateToStr:
    @pytest.mark.parametrize(
        "input, expected",
        [(date(2021, 10, 3), "20211003"), ("banana", "banana"), (10, 10)],
    )
    def test_convert_possible_date_to_str(self, input, expected):
        result = convert_possible_date_to_str(input)
        assert result == expected
