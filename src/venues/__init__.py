from venues.prizepicks import PrizePicksAdapter

_REGISTRY = {a.name: a for a in [PrizePicksAdapter()]}


def get_venue(name):
    return _REGISTRY[name]
