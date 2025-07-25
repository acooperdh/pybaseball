from typing import Callable

import polars as pl
import pytest
import requests

from pybaseball.team_fielding import team_fielding


@pytest.fixture()
def sample_html(get_data_file_contents: Callable[[str], str]) -> str:
    return get_data_file_contents('team_fielding.html')


@pytest.fixture()
def sample_processed_result(get_data_file_dataframe: Callable) -> pl.DataFrame:
    return get_data_file_dataframe('team_fielding.csv')


def test_team_fielding(response_get_monkeypatch: Callable, sample_html: str, sample_processed_result: pl.DataFrame) -> None:
    season = 2019

    response_get_monkeypatch(sample_html)

    team_fielding_result = team_fielding(season).reset_index(drop=True)

    pl.testing.assert_frame_equal(team_fielding_result, sample_processed_result, check_dtype=False)
