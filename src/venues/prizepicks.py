from extractors.pp_extractors import fetch_live_board
from venues.base import VenueAdapter


class PrizePicksAdapter(VenueAdapter):
    name = "prizepicks"

    def fetch_board(self, league="NBA"):
        # league param plumbed through in the NFL plan (extractor currently
        # hardcodes the NBA filter).
        return fetch_live_board()
