from sqlalchemy.orm import configure_mappers

from src.db.models.dv.hub_leagues import HubLeagues
from src.db.models.dv.hub_seasons import HubSeasons
from src.db.models.dv.hub_teams import HubTeamns
from src.db.models.dv.hub_matches import HubMatches
from src.db.models.dv.lnk_matches import LnkMatches
from src.db.models.dv.sat_matches_core_results import SatMatchesCoreResults
from src.db.models.dv.sat_matches_core_statistics import SatMatchesCoreStatistics
from src.db.models.dv.sat_matches_odds_bet365 import SatMatchesOddsBet365
from src.db.models.dv.sat_matches_odds_market_consensus import SatMatchesOddsMarketConsensus
from src.db.models.dv.sat_matches_odds_sportingbet import SatMatchesOddsSportingbet
from src.db.models.dv.monitor_logs import MonitorLogs

configure_mappers()
