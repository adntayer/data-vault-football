# python -m src.data_vault.football_co.fixtures
# source = https://www.football-data.co.uk/notes.txt

DICT_MATCH_RESULTS = {
    'FTHG': 'Full Time Home Team Goals',
    # 'HG': 'Full Time Home Team Goals',
    'FTAG': 'Full Time Away Team Goals',
    # 'AG': 'Full Time Away Team Goals',
    'FTR': 'Full Time Result (H=Home Win, D=Draw, A=Away Win)',
    # 'Res': 'Full Time Result (H=Home Win, D=Draw, A=Away Win)',
    'HTHG': 'Half Time Home Team Goals',
    'HTAG': 'Half Time Away Team Goals',
    'HTR': 'Half Time Result (H=Home Win, D=Draw, A=Away Win)',
}

DICT_MATCH_STATISTICS = {
    'Attendance': 'Crowd Attendance',
    'Referee': 'Match Referee',
    'HS': 'Home Team Shots',
    'AS': 'Away Team Shots',
    'HST': 'Home Team Shots on Target',
    'AST': 'Away Team Shots on Target',
    'HHW': 'Home Team Hit Woodwork',
    'AHW': 'Away Team Hit Woodwork',
    'HC': 'Home Team Corners',
    'AC': 'Away Team Corners',
    'HF': 'Home Team Fouls Committed',
    'AF': 'Away Team Fouls Committed',
    'HFKC': 'Home Team Free Kicks Conceded',
    'AFKC': 'Away Team Free Kicks Conceded',
    'HO': 'Home Team Offsides',
    'AO': 'Away Team Offsides',
    'HY': 'Home Team Yellow Cards',
    'AY': 'Away Team Yellow Cards',
    'HR': 'Home Team Red Cards',
    'AR': 'Away Team Red Cards',
    'HBP': 'Home Team Bookings Points (10 = yellow, 25 = red)',
    'ABP': 'Away Team Bookings Points (10 = yellow, 25 = red)',
}

DICT_ODDS_BET365 = {
    'B365H': 'Bet365 home win odds',
    'B365D': 'Bet365 draw odds',
    'B365A': 'Bet365 away win odds',
    'B365CH': 'Bet365 home win closing odds',
    'B365CD': 'Bet365 draw closing odds',
    'B365CA': 'Bet365 away win closing odds',
    'B365>2.5': 'Bet365 over 2.5 goals',
    'B365<2.5': 'Bet365 under 2.5 goals',
}

DICT_ODDS_BETBRAIN = {
    'BbMxH': 'Betbrain maximum home win odds',
    'BbAvH': 'Betbrain average home win odds',
    'BbMxD': 'Betbrain maximum draw odds',
    'BbAvD': 'Betbrain average draw odds',
    'BbMxA': 'Betbrain maximum away win odds',
    'BbAvA': 'Betbrain average away win odds',
    'BbOU': 'Number of BetBrain bookmakers used to calculate over/under 2.5 goals averages and maximums',
    'BbMx>2.5': 'Betbrain maximum over 2.5 goals',
    'BbAv>2.5': 'Betbrain average over 2.5 goals',
    'BbMx<2.5': 'Betbrain maximum under 2.5 goals',
    'BbAv<2.5': 'Betbrain average under 2.5 goals',
}

DICT_ODDS_BETFAIR = {
    'BFH': 'Betfair home win odds',
    'BFD': 'Betfair draw odds',
    'BFA': 'Betfair away win odds',
    'BFCH': 'Betfair home win closing odds',
    'BFCD': 'Betfair draw closing odds',
    'BFEA': 'Betfair Exchange away win odds',
    'BFEH': 'Betfair Exchange home win odds',
    'BFED': 'Betfair Exchange draw odds',
}

DICT_ODDS_SPORTINGBET = {
    'SBH': 'Sportingbet home win odds',
    'SBD': 'Sportingbet draw odds',
    'SBA': 'Sportingbet away win odds',
    'SBCH': 'Sportingbet home win closing odds',
    'SBCD': 'Sportingbet draw closing odds',
    'SBAC': 'Sportingbet away win closing odds',
}

DICT_ODDS_BLUE_SQUARE = {
    'BSH': 'Blue Square home win odds',
    'BSD': 'Blue Square draw odds',
    'BSA': 'Blue Square away win odds',
    'BSCH': 'Blue Square home win closing odds',
    'BSDC': 'Blue Square draw closing odds',
    'BSAC': 'Blue Square away win closing odds',
}

DICT_ODDS_GAMEBOOKERS = {
    'GBH': 'Gamebookers home win odds',
    'GBD': 'Gamebookers draw odds',
    'GBA': 'Gamebookers away win odds',
    'GBCH': 'Gamebookers home win closing odds',
    'GBCD': 'Gamebookers draw closing odds',
    'GBCA': 'Gamebookers away win closing odds',
    'GB>2.5': 'Gamebookers over 2.5 goals',
    'GB<2.5': 'Gamebookers under 2.5 goals',
}

DICT_ODDS_INTERWETTEN = {
    'IWH': 'Interwetten home win odds',
    'IWD': 'Interwetten draw odds',
    'IWA': 'Interwetten away win odds',
    'IWHC': 'Interwetten home win closing odds',
    'IWDC': 'Interwetten draw closing odds',
    'IWAC': 'Interwetten away win closing odds',
}

DICT_ODDS_LADBROKES = {
    'LBH': 'Ladbrokes home win odds',
    'LBD': 'Ladbrokes draw odds',
    'LBA': 'Ladbrokes away win odds',
    'LBCH': 'Ladbrokes home win closing odds',
    'LBDC': 'Ladbrokes draw closing odds',
    'LBCA': 'Ladbrokes away win closing odds',
}

DICT_ODDS_PINNACLE = {
    'PSH': 'Pinnacle home win odds',
    'PSD': 'Pinnacle draw odds',
    'PSA': 'Pinnacle away win odds',
    'PSCH': 'Pinnacle home win closing odds',
    'PSDC': 'Pinnacle draw closing odds',
    'PSAC': 'Pinnacle away win closing odds',
    'P>2.5': 'Pinnacle over 2.5 goals',
    'P<2.5': 'Pinnacle under 2.5 goals',
}

DICT_ODDS_SPORTING = {
    'SOH': 'Sporting Odds home win odds',
    'SOD': 'Sporting Odds draw odds',
    'SOA': 'Sporting Odds away win odds',
    'SOHC': 'Sporting Odds home win closing odds',
    'SODC': 'Sporting Odds draw closing odds',
    'SOAC': 'Sporting Odds away win closing odds',
}

DICT_ODDS_STAN_JAMES = {
    'SJH': 'Stan James home win odds',
    'SJD': 'Stan James draw odds',
    'SJA': 'Stan James away win odds',
    'SJCH': 'Stan James home win closing odds',
    'SJCD': 'Stan James draw closing odds',
    'SJCA': 'Stan James away win closing odds',
}

DICT_ODDS_STANLEYBET = {
    'SYH': 'Stanleybet home win odds',
    'SYD': 'Stanleybet draw odds',
    'SYA': 'Stanleybet away win odds',
    'SYCH': 'Stanleybet home win closing odds',
    'SYCD': 'Stanleybet draw closing odds',
    'SYCA': 'Stanleybet away win closing odds',
}

DICT_ODDS_VC_BET = {
    'VCH': 'VC Bet home win odds',
    'VCD': 'VC Bet draw odds',
    'VCA': 'VC Bet away win odds',
    'VCHC': 'VC Bet home win closing odds',
    'VCDC': 'VC Bet draw closing odds',
    'VCAC': 'VC Bet away win closing odds',
}

DICT_ODDS_WILLIAM_HILL = {
    'WHH': 'William Hill home win odds',
    'WHD': 'William Hill draw odds',
    'WHA': 'William Hill away win odds',
    'WHHC': 'William Hill home win closing odds',
    'WHCD': 'William Hill draw closing odds',
    'WHCA': 'William Hill away win closing odds',
}

DICT_ODDS_MARKET_CONSENSUS = {
    'MaxH': 'Market maximum home win odds',
    'MaxCH': 'Market maximum home win closing odds',
    'MaxD': 'Market maximum draw win odds',
    'MaxCD': 'Market maximum draw win closing odds',
    'MaxA': 'Market maximum away win odds',
    'MaxCA': 'Market maximum away win closing odds',
    'AvgH': 'Market average home win odds',
    'AvgCH': 'Market average home win closing odds',
    'AvgD': 'Market average draw win odds',
    'AvgCD': 'Market average draw win closing odds',
    'AvgA': 'Market average away win odds',
    'AvgCA': 'Market average away win closing odds',
    'Max>2.5': 'Market maximum over 2.5 goals',
    'MaxC>2.5': 'Market maximum over 2.5 goals - closing odds',
    'Max<2.5': 'Market maximum under 2.5 goals',
    'MaxC<2.5': 'Market maximum under 2.5 goals - closing odds',
    'Avg>2.5': 'Market average over 2.5 goals',
    'AvgC>2.5': 'Market average over 2.5 goals - closing odds',
    'Avg<2.5': 'Market average under 2.5 goals',
    'AvgC<2.5': 'Market average under 2.5 goals - closing odds',
}

DICT_ODDS_ASIAN_HANDICAP = {
    'BbAH': 'Number of BetBrain bookmakers used to Asian handicap averages and maximums',
    'BbAHh': 'Betbrain size of handicap (home team)',
    'AHh': 'Market size of handicap (home team)',
    'BbMxAHH': 'Betbrain maximum Asian handicap home team odds',
    'BbAvAHH': 'Betbrain average Asian handicap home team odds',
    'BbMxAHA': 'Betbrain maximum Asian handicap away team odds',
    'BbAvAHA': 'Betbrain average Asian handicap away team odds',
    'GBAHH': 'Gamebookers Asian handicap home team odds',
    'GBAHA': 'Gamebookers Asian handicap away team odds',
    'GBAH': 'Gamebookers size of handicap (home team)',
    'LBAHH': 'Ladbrokes Asian handicap home team odds',
    'LBAHA': 'Ladbrokes Asian handicap away team odds',
    'LBAH': 'Ladbrokes size of handicap (home team)',
    'B365AHH': 'Bet365 Asian handicap home team odds',
    'B365AHA': 'Bet365 Asian handicap away team odds',
    'B365AH': 'Bet365 size of handicap (home team)',
    'PAHH': 'Pinnacle Asian handicap home team odds',
    'PAHA': 'Pinnacle Asian handicap away team odds',
    'MaxAHH': 'Market maximum Asian handicap home team odds',
    'MaxAHA': 'Market maximum Asian handicap away team odds',
    'AvgAHH': 'Market average Asian handicap home team odds',
    'AvgAHA': 'Market average Asian handicap away team odds',
}
