import json
import re
from datetime import date, datetime
from hashlib import md5


def serialdate(obj):
    if isinstance(obj, (datetime, date)):
        return str(obj)
    raise TypeError("not serializable", type(obj))


def getsufix(pid):
    r = re.findall(r"[A-Za-z]*(\d+.*)", pid)
    if len(r) == 0:
        return ""
    return r[0].lstrip("0").strip()


def getprefix(pid):
    return re.findall(r"([A-Za-z]+)\d*", pid)[0].strip()


def addhash(doc):
    """
    >>> from bson import ObjectId
    >>> measured = 0 #isoparse("2021-12-22T23:08:23.019Z".split("Z")[0].split("+")[0])
    >>> #doc = {'_id': ObjectId('61c50d81853a98007290388b'), 'participantId': 'aPROSITSM_xxx', 'attribute': 'notifications__action_flags_package', 'value': ["post", "autoCancel", "com.soundcloud.android"], 'measuredAt': measured , 'uploadedAt': datetime(2022, 1, 2, 0, 0, 6, 348000)}
    >>> addhash(doc)["hash"]
    '98378edced91c2a75fb6276c6c674959'
    """
    _id, up = doc.pop("_id"), doc.pop("uploadedAt")
    brand = doc.pop("brand") if "brand" in doc else None
    txt = json.dumps(doc, sort_keys=True, default=serialdate)
    doc["hash"] = md5(txt.encode()).hexdigest()
    doc["uploadedAt"] = up
    doc["_id"] = _id
    if brand is not None:
        doc["brand"] = brand
    return doc


def fixpid(doc):
    """
    >>> fixpid({"participantId": "PROSITC0001"})["participantId"]
    'PROSITC_1'
    >>> fixpid({"participantId": "PROSITC0001A"})["participantId"]
    'PROSITC_1A'
    >>> fixpid({"participantId": "PRO"})["participantId"]
    'PRO_'
    >>> fixpid({"participantId": "PROSITC000BC"})["participantId"]
    'PROSITC_BC'
    >>> fixpid({"participantId": "PROSITC000BC3"})["participantId"]
    'PROSITC_BC3'
    """
    pid = doc["participantId"]
    doc["participantId"] = getprefix(pid) + "_" + getsufix(pid)
    return doc


def isnumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
