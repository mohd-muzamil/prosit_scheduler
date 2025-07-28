from datetime import datetime, timedelta


def bydate(start, days, **kwargs):
    end = start + timedelta(days=days)
    criteria = {"measuredAt": {"$gte": start, "$lt": end}}
    criteria.update(kwargs)
    return criteria
