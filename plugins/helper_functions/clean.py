from helper_functions.crypt import decode, dic2str, encrypt
from helper_functions.device import aattributes, iattributes
from helper_functions.format import addhash


def addsuffix(doc, branded):
    """Return None if doc hasn't a clearly defined device brand yet. In this case, postpone it by inserting it into unbranded."""
    pid = doc["participantId"]
    if doc["attribute"] in aattributes:
        doc["brand"] = "a"
        if pid not in branded:
            branded[pid] = f"a{pid}"
        elif branded[pid][0] != "a":
            branded[pid] = f"x{pid}"
    elif doc["attribute"] in iattributes:
        doc["brand"] = "i"
        if pid not in branded:
            branded[pid] = f"i{pid}"
        elif branded[pid][0] != "i":
            branded[pid] = f"x{pid}"
    elif pid in branded:
        doc["brand"] = branded[pid][0]
    else:
        return None
    return doc


def fixdoc(doc, decrypted_locations, loo, hashes):
    """Return None if doc is a duplicate."""
    if doc is None:
        raise Exception("Unexpected doc=None, probably something wrong with unbranded docs handling")
    if "start_time" in doc:
        raise Exception(f"Old format (arrays) detected! {doc}")

    # new location format
    if "location" in doc["attribute"].lower():
        doc["attribute"] = "location"
        decoded = decode(doc["value"], personal_cache=loo, readonly=True)
        stringfied = dic2str(decoded)
        doc["value"] = stringfied
        addhash(doc)
        encrypted = encrypt(stringfied, new=True).decode()
        decrypted_locations[encrypted] = stringfied
        doc["value"] = encrypted
        ret = doc
    else:
        ret = addhash(doc)

    h = doc["hash"]
    if h in hashes:
        if "location" in doc["attribute"].lower():
            del decrypted_locations[encrypted]
        return None
    hashes.add(h)

    return ret
