import polars as pl

from pybaseball.statcast_fielding import (
	statcast_outs_above_average,
	statcast_outfield_directional_oaa,
	statcast_outfield_catch_prob,
	statcast_outfielder_jump,
	statcast_catcher_poptime,
	statcast_catcher_framing,
	statcast_fielding_run_value
)

def test_statcast_outs_above_average() -> None:
	min_att = 50
	pos = "of"
	result: pl.DataFrame = statcast_outs_above_average(2019, pos, min_att)

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 16
	assert len(result) > 0

def test_statcast_outs_above_average_view() -> None:
	min_att = 50
	pos = "of"
	view = "Pitcher"
	result: pl.DataFrame = statcast_outs_above_average(2019, pos, min_att, view)

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 17
	assert len(result) > 0

def test_statcast_outfield_directional_oaa() -> None:
	min_opp = 50
	result: pl.DataFrame = statcast_outfield_directional_oaa(2019, min_opp)

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 13
	assert len(result) > 0
	assert len(result.loc[result.attempts < min_opp]) == 0

def test_statcast_outfield_catch_prob() -> None:
	min_opp = 25
	result: pl.DataFrame = statcast_outfield_catch_prob(2019, min_opp)

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 19
	assert len(result) > 0

def test_statcast_outfielder_jump() -> None:		
	min_att = 50
	result: pl.DataFrame = statcast_outfielder_jump(2019, min_att)
	
	assert result is not None
	assert not result.empty

	assert len(result.columns) == 13
	assert len(result) > 0
	assert len(result.loc[result.n < min_att]) == 0

def test_statcast_catcher_poptime() -> None:
	min_2b_att = 5
	min_3b_att = 0
	result: pl.DataFrame = statcast_catcher_poptime(2019, min_2b_att, min_3b_att) 

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 14
	assert len(result) > 0
	assert len(result.loc[result.pop_2b_sba_count < min_2b_att]) == 0
	assert len(result.loc[result.pop_3b_sba_count < min_3b_att]) == 0

def test_statcast_catcher_framing() -> None:
	min_called_p = 100
	result: pl.DataFrame = statcast_catcher_framing(2019, min_called_p)

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 15
	assert len(result) > 0
	assert len(result.loc[result.n_called_pitches < min_called_p]) == 0

def test_statcast_fielding_run_value() -> None:
	min_inn = 50
	pos = 4
	result: pl.DataFrame = statcast_fielding_run_value(2019, pos, min_inn)

	assert result is not None
	assert not result.empty

	assert len(result.columns) == 22
	assert len(result) > 0

#test_statcast_outs_above_average_view()
test_statcast_outs_above_average()
