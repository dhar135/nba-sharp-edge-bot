from abc import ABC, abstractmethod
import pandas as pd

BOARD_COLUMNS = ["Player", "Stat", "Line", "Matchup", "Game Date"]


class VenueAdapter(ABC):
    name: str

    @abstractmethod
    def fetch_board(self, league: str) -> pd.DataFrame:
        """Return the live prop board with at least BOARD_COLUMNS."""
