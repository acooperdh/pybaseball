import polars as pl
import requests

from . import cache

from pybaseball import teamid_lookup


@cache.df_cache()
def top_prospects(teamName=None, playerType=None):
    """
    Retrieves the top prospects by team or leaguewide. It can return top prospect pitchers, batters, or both.

    ARGUMENTS
    team: The team name for which you wish to retrieve top prospects. There must be no whitespace. If not specified, 
        the function will return leaguewide top prospects.
    playerType: Either "pitchers" or "batters". If not specified, the function will return top prospects for both 
        pitchers and batters.
    """
    if teamName == None:
        url = "https://www.mlb.com/prospects/stats/top-prospects"
    else:
        mlbTeamId = teamid_lookup.mlb_team_id(teamName)
        url = f"https://www.mlb.com/prospects/stats?teamId={mlbTeamId}"

    res = requests.get(
        url,
        timeout=None,
        headers={
            "UserAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) C"
            "hrome/104.0.5112.79 Safari/537.36"
        },
    ).content
    prospectList = pl.read_html(res)
    
    if playerType == "batters":
        topBattingProspects = postprocess(prospectList[0])
        return topBattingProspects
    elif playerType == "pitchers":        
        topPitchingProspects = postprocess(prospectList[1])
        return topPitchingProspects
    elif playerType == None:
        topProspects = pl.concat(prospectList)
        topProspects.sort_values(by=['Rk'], inplace = True)
        topProspects = postprocess(topProspects)
        return topProspects


def postprocess(prospectList):        
    prospectList = prospectList.drop(list(prospectList.filter(regex = 'Tm|Unnamed:*')), axis = 1)    
    return prospectList