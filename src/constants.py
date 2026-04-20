# src/constants.py
# Centralized list of supported PrizePicks stats to avoid duplication across files

SUPPORTED_STATS = [
    "Points",
    "Rebounds", 
    "Assists",
    "Pts+Rebs+Asts",
    "3-PT Made",
    "Blocked Shots",
    "Steals",
    "Turnovers",
    "Blks+Stls",
    "Pts+Rebs",
    "Pts+Asts",
    "Rebs+Asts"
]

# Mapping from PrizePicks stat names to NBA API column names or calculations
STAT_MAPPING = {
    "Pts+Rebs+Asts": "PRA",
    "Points": "PTS",
    "Rebounds": "REB",
    "Assists": "AST",
    "3-PT Made": "FG3M",
    "Blocked Shots": "BLK",
    "Steals": "STL",
    "Turnovers": "TOV",
    "Blks+Stls": "BS",
    "Pts+Rebs": "PR",
    "Pts+Asts": "PA",
    "Rebs+Asts": "RA"
}

def calculate_actual(stat_type, game_row):
    """
    Calculate the actual stat value from the game row based on stat_type.
    """
    pts = float(game_row.iloc[0]['PTS'])
    reb = float(game_row.iloc[0]['REB'])
    ast = float(game_row.iloc[0]['AST'])
    fg3m = float(game_row.iloc[0]['FG3M'])
    blk = float(game_row.iloc[0]['BLK'])
    stl = float(game_row.iloc[0]['STL'])
    tov = float(game_row.iloc[0]['TOV'])
    
    if stat_type == "Points":
        return pts
    elif stat_type == "Rebounds":
        return reb
    elif stat_type == "Assists":
        return ast
    elif stat_type == "Pts+Rebs+Asts":
        return pts + reb + ast
    elif stat_type == "3-PT Made":
        return fg3m
    elif stat_type == "Blocked Shots":
        return blk
    elif stat_type == "Steals":
        return stl
    elif stat_type == "Turnovers":
        return tov
    elif stat_type == "Blks+Stls":
        return blk + stl
    elif stat_type == "Pts+Rebs":
        return pts + reb
    elif stat_type == "Pts+Asts":
        return pts + ast
    elif stat_type == "Rebs+Asts":
        return reb + ast
    else:
        return 0.0