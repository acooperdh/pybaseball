import polars as pl

from tests.conftest import CURRENT_SC_COLUMNS

from pybaseball.statcast_batter import (
    statcast_batter,
    statcast_batter_exitvelo_barrels,
    statcast_batter_expected_stats,
    statcast_batter_percentile_ranks,
    statcast_batter_pitch_arsenal,
    statcast_batter_bat_tracking
)


def test_statcast_batter_exitvelo_barrels() -> None:
    min_bbe = 250
    result: pl.DataFrame = statcast_batter_exitvelo_barrels(2019, min_bbe)

    assert result is not None
    assert not result.empty

    assert len(result.columns) == 18
    assert len(result) > 0
    assert len(result[result['attempts'] < min_bbe]) == 0


def test_statcast_batter() -> None:
    result: pl.DataFrame = statcast_batter('2019-01-01', '2019-12-31', 642715)

    assert result is not None
    assert not result.empty

    assert len(result.columns) == CURRENT_SC_COLUMNS
    assert len(result) > 0

def test_statcast_batter_expected_stats() -> None:
    min_pa = 250
    result: pl.DataFrame = statcast_batter_expected_stats(2019, min_pa)

    assert result is not None
    assert not result.empty

    assert len(result.columns) == 15
    assert len(result) > 0
    assert len(result[result['pa'] < min_pa]) == 0

def test_statcast_batter_percentile_ranks() -> None:
    result: pl.DataFrame = statcast_batter_percentile_ranks(2019)

    assert result is not None
    assert not result.empty

    assert len(result.columns) == 17
    assert len(result) > 0

def test_statcast_batter_pitch_arsenal() -> None:
    min_pa = 25
    result: pl.DataFrame = statcast_batter_pitch_arsenal(2019, min_pa)

    assert result is not None
    assert not result.empty

    assert len(result.columns) == 21
    assert len(result) > 0
    assert len(result[result['pa'] < min_pa]) == 0
def test_statcast_batter_bat_tracking() -> None:
    min_pa = 25
    result: pl.DataFrame = statcast_batter_bat_tracking(2024, min_pa)

    assert result is not None
    assert not result.empty

    assert len(result.columns) == 18
    assert len(result) > 0
    assert len(result[result['swings_competitive'] < min_pa]) == 0
