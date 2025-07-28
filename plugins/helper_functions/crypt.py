# from sqlite3 import OperationalError

import base64

import binascii
import rncryptor as rn
# from shelchemy.cache import sopen
# from sqlalchemy.exc import OperationalError
from helper_functions.format import isnumber


def dic2str(dic):
    return " | ".join(str(v) for v in dic.values())


def encode(dic, new=False):
    """
    >>> d = {'latitude': 43.6372664, 'longitude': -64.5873868, 'altitude': 31.100000381469731, 'degree': 186.33613, 'speed': 0.80074306, 'accuracy': 10.109, 'address': 'Henry Street, Halifax, Nova Scotia, B3H 3J6, Canada'}
    >>> decode(encode(d), "/tmp/cache.db") == d
    True
    """
    return encrypt(dic2str(dic), new).decode()


def encrypt(txt, new=False):
    return base64.b64encode(rn.encrypt(txt, "0T1u2p3p4e5r6" if new else "location"))


def handle_item(ll, personal_cache, new, readonly):
    if isinstance(ll, str):
        if ll.startswith("[B@") or ll == "":
            return None
        elif isnumber(ll):
            return float(ll)
        else:
            return decrypt(ll, personal_cache, new, readonly=readonly)
    else:
        return float(ll)


def handle_none(x):
    if x == "None":
        return None
    return float(x)


def decode(location, personal_cache, new=False, readonly=False):
    """
    Newer locations will be calculated on-demand and added to your personal cache
    Order: latitude,longitude,altitude,degree,speed,accuracy,address

    >>> decode("AwEC8irqTGTCqj0g9zv+ZqBhPgveOL35+jzha79v6JHGJiHNYuZKm6Q0laTqTte6yiyjYR9DgwFdLOPIQaPatl0wQLpHWG00bUbQ5Jj98IYJuoYSez4KuufQxSdQuIlzqVI9xNAnvYbVLNEr+bXZRit3OA9UpouKET+UQuf+UxNQ8YIcLlmVRPXhsJ3YJukMCj1rI/1PdZnmvTlyQW+Znf0XZQVkWpW/wAcNfVpcrJRUUi5UIkdX28cbi2CZqAPbZ0c=", "/tmp/cache.db")
    {'latitude': 44.6366244, 'longitude': -63.572621, 'altitude': -2.799999713897705, 'degree': 0.0, 'speed': 0.0, 'accuracy': 12.431, 'address': 'Victoria Road, Halifax, Nova Scotia, B3H 4K5, Canada'}

    Install:
    pip install rncryptor idict pandas sqlalchemy
    """
    old = location
    if isinstance(location, str):
        location = [location]

    if not isinstance(location, list):
        raise Exception(f"\nUnknown location format »»\n{location}\n««»»»»\n{old}\n««««'")

    try:
        if len(location) == 1 and isinstance(location[0], str):
            if location[0] in ["", "0.0", "0"]:
                return {"latitude": None, "longitude": None, "altitude": None, "degree": None, "speed": None, "accuracy": None, "address": None}
            if len(location[0]) < 15 and isnumber(location[0]):
                n = float(location[0])
                return {"latitude": n if n > 0 else None, "longitude": n if n < 0 else None, "altitude": None, "degree": None, "speed": None, "accuracy": None, "address": None}
            if len(location[0].split(",")) > 1:
                return {"latitude": None, "longitude": None, "altitude": None, "degree": None, "speed": None, "accuracy": None, "address": location[0]}
            # '44.6373059 | -63.5871963 | 33.79999923706055 | 209.81808 | 1.1425375 | 20.49 | University Avenue, Halifax, Nova Scotia, B3H 1W5, Canada'
            location = decrypt(location[0], personal_cache, new, readonly=readonly).split(" | ")
        if location[0] in ["", "0.0", "0"]:
            return {"latitude": None, "longitude": None, "altitude": None, "degree": None, "speed": None, "accuracy": None, "address": None}

        if len(location) == 7:
            if isnumber(location[5]) and not isnumber(location[6]) or all(isinstance(x, str) for x in location):
                return {"latitude": handle_none(location[0]), "longitude": handle_none(location[1]), "altitude": handle_none(location[2]), "degree": handle_none(location[3]), "speed": handle_none(location[4]), "accuracy": handle_none(location[5]), "address": location[6]}
            if not isnumber(location[5]) and isnumber(location[6]):
                # ['8.7', '-1.500000023841858', '0.0', '44.0366317', '-63.872065', 'Victoria Road, Halifax, Nova Scotia, B3H 4K3, Canada', '0.0']
                return {"latitude": handle_none(location[3]), "longitude": handle_none(location[4]), "altitude": handle_none(location[1]), "degree": handle_none(location[2]), "speed": handle_none(location[6]), "accuracy": handle_none(location[0]), "address": location[5]}
        if len(location) == 6:
            # in:   accuracy    altitude    bearingDeg  latitude    longitude   speed
            return {
                "latitude": handle_item(location[3], personal_cache, new, readonly),
                "longitude": handle_item(location[4], personal_cache, new, readonly),
                "altitude": handle_item(location[1], personal_cache, new, readonly),
                "degree": handle_item(location[2], personal_cache, new, readonly),
                "speed": handle_item(location[5], personal_cache, new, readonly),
                "accuracy": handle_item(location[0], personal_cache, new, readonly),
                "address": None
            }
        if len(location) == 3 and not isnumber(location[2]):
            # in:   latitude    longitude   address
            return {"latitude": handle_none(location[0]), "longitude": handle_none(location[1]), "altitude": None, "degree": None, "speed": None, "accuracy": None, "address": location[2]}
    except Exception as e:
        raise Exception(f"\nCouldn't process format »\n{location}\n««»»»»\n{old}\n««««\n>>>>\n{e}\n<<<<'")
    raise Exception(f"\nUnknown location format »\n{location}\n««»»»»\n{old}\n««««'")


def decrypt(locationstr, personal_cache, new, readonly):
    """
    Newer locations will be calculated on-demand and added to your personal cache

    >>> s = "AwEC8irqTGTCqj0g9zv+ZqBhPgveOL35+jzha79v6JHGJiHNYuZKm6Q0laTqTte6yiyjYR9DgwFdLOPIQaPatl0wQLpHWG00bUbQ5Jj98IYJuoYSez4KuufQxSdQuIlzqVI9xNAnvYbVLNEr+bXZRit3OA9UpouKET+UQuf+UxNQ8YIcLlmVRPXhsJ3YJukMCj1rI/1PdZnmvTlyQW+Znf0XZQVkWpW/wAcNfVpcrJRUUi5UIkdX28cbi2CZqAPbZ0c="
    >>> import rncryptor as rn
    >>> rn.decrypt(base64.b64decode(s), "location")

    Install:
    pip install rncryptor pandas shelchemy
    """

    try:
        if not isinstance(personal_cache, str):
            if locationstr in personal_cache:
                return personal_cache[locationstr].decode()
            txt = base64.b64decode(locationstr)
            res = rn.decrypt(txt, "0T1u2p3p4e5r6" if new else "location")
            if not readonly:
                personal_cache[locationstr] = res
            return res
        with sopen(f"sqlite+pysqlite:///{personal_cache}", autopack=False, ondup="skip") as so:
            if locationstr in so:
                return so[locationstr].decode()
            txt = base64.b64decode(locationstr)
            res = rn.decrypt(txt, "0T1u2p3p4e5r6" if new else "location")
            if not readonly:
                so[locationstr] = res
        return res
    except OperationalError:
        print(f"ERROR: Please, see if file {personal_cache} exists.")
        exit()
    except binascii.Error:
        raise B64Exc(f"ERROR: Cannot decode b64: {locationstr}.")
    except rn.DecryptionError:
        raise DecrypExc(f"ERROR: Cannot decrypt: {locationstr} {base64.b64decode(locationstr)}.")


# mp.ProcessingPool().imap(thread, chunks(db, conames_))

class B64Exc(Exception):
    pass


class DecrypExc(Exception):
    pass
