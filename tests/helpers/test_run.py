from pathlib import Path
from freezegun import freeze_time
import pytest
from src.helpers.run import Run


class TestRun:
    @pytest.fixture(scope="class")
    @freeze_time("2021-01-01")
    def mock_run(self):
        run = Run()
        run.datetime_now = "2021-01-01-00-00-00"
        return run

    def test_run_has_timestamp(self, mock_run):
        result = mock_run.start_datetime
        expected = "2021-01-01-00-00-00"
        assert result == expected

    def test_create_run_filepath(self, mock_run):
        result = mock_run.create_run_filepath("my_folder/my_subfolder/")
        expected = Path("my_folder/my_subfolder/2021-01-01-00-00-00")
        assert result == expected

    def test_create_run_filepath__no_filepath(self, mock_run):
        result = mock_run.create_run_filepath()
        expected = Path("2021-01-01-00-00-00")
        assert result == expected
