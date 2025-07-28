# from datetime import datetime, timedelta
# from math import isnan

# from dateutil.utils import today
# from geopy.distance import geodesic
# from idict import idict
# from pymongo import MongoClient
# from shelchemy.cache import sopen
# from shelchemy.locker import locker

# from server.config import config
# from server.crypt import decode
# from server.db import insert
# from server.format import getsufix, getprefix
# from server.tools import bydate

# # https://www.cdc.gov/nchs/data/ad/ad347.pdf  https://www.cdc.gov/nchs/data/nhsr/nhsr122-508.pdf
# wbyage_female = {2: 13.3, 3: 15.2, 4: 17.9, 5: 20.6, 6: 22.4, 7: 25.9, 8: 31.9, 9: 35.4, 10: 40.0, 11: 47.9, 12: 52.0, 13: 57.7, 14: 59.9, 15: 61.1, 16: 63.0, 17: 61.7, 18: 65.2, 19: 67.9,
#                  20: 76, 21: 76, 22: 76, 23: 76, 24: 76, 25: 76, 26: 76, 27: 76, 28: 76, 29: 76, 30: 76, 31: 76, 32: 76, 33: 76, 34: 76, 35: 76, 36: 76, 37: 76, 38: 76, 39: 76,
#                  40: 80, 41: 80, 42: 80, 43: 80, 44: 80, 45: 80, 46: 80, 47: 80, 48: 80, 49: 80, 50: 80, 51: 80, 52: 80, 53: 80, 54: 80, 55: 80, 56: 80, 57: 80, 58: 80, 59: 80,
#                  60: 75.5, 61: 75.5, 62: 75.5, 63: 75.5, 64: 75.5, 65: 75.5, 66: 75.5, 67: 75.5, 68: 75.5, 69: 75.5, 70: 75.5, 71: 75.5, 72: 75.5, 73: 75.5, 74: 75.5, 75: 75.5, 76: 75.5, 77: 75.5, 78: 75.5, 79: 75.5,
#                  80: 75.5, 81: 75.5, 82: 75.5, 83: 75.5, 84: 75.5, 85: 75.5, 86: 75.5, 87: 75.5, 88: 75.5, 89: 75.5, 90: 75.5, 91: 75.5, 92: 75.5, 93: 75.5, 94: 75.5, 95: 75.5, 96: 75.5, 97: 75.5, 98: 75.5, 99: 75.5}
# wbyage_male = {2: 13.7, 3: 15.9, 4: 18.5, 5: 21.3, 6: 23.5, 7: 27.2, 8: 32.7, 9: 36.0, 10: 38.6, 11: 43.7, 12: 50.4, 13: 53.9, 14: 63.9, 15: 68.3, 16: 74.4, 17: 75.6, 18: 75.6, 19: 78.2,
#                20: 89.3, 21: 89.3, 22: 89.3, 23: 89.3, 24: 89.3, 25: 89.3, 26: 89.3, 27: 89.3, 28: 89.3, 29: 89.3, 30: 89.3, 31: 89.3, 32: 89.3, 33: 89.3, 34: 89.3, 35: 89.3, 36: 89.3, 37: 89.3, 38: 89.3, 39: 89.3,
#                40: 91.1, 41: 91.1, 42: 91.1, 43: 91.1, 44: 91.1, 45: 91.1, 46: 91.1, 47: 91.1, 48: 91.1, 49: 91.1, 50: 91.1, 51: 91.1, 52: 91.1, 53: 91.1, 54: 91.1, 55: 91.1, 56: 91.1, 57: 91.1, 58: 91.1, 59: 91.1,
#                60: 88.3, 61: 88.3, 62: 88.3, 63: 88.3, 64: 88.3, 65: 88.3, 66: 88.3, 67: 88.3, 68: 88.3, 69: 88.3, 70: 88.3, 71: 88.3, 72: 88.3, 73: 88.3, 74: 88.3, 75: 88.3, 76: 88.3, 77: 88.3, 78: 88.3, 79: 88.3,
#                80: 88.3, 81: 88.3, 82: 88.3, 83: 88.3, 84: 88.3, 85: 88.3, 86: 88.3, 87: 88.3, 88: 88.3, 89: 88.3, 90: 88.3, 91: 88.3, 92: 88.3, 93: 88.3, 94: 88.3, 95: 88.3, 96: 88.3, 97: 88.3, 98: 88.3, 99: 88.3}


# def gen():
#     start_ = datetime.strptime("2019-10-01", "%Y-%m-%d")
#     while start_ < today():
#         yield str(start_).split(" ")[0]
#         start_ += timedelta(days=1)


# d = idict.fromfile("/home/davi/davedata3.csv", output_format="df")
# df = d.df
# print(df)

# speed_cutoff = 15  # km/h      6.0 mph = running a mile in 10 minutes - vigorous activity
# weight, gender = {}, {}
# for pid, age, sex in zip(df["record_id"], df["age"], df["sex"]):
#     fixedpid = getprefix(pid) + "_" + getsufix(pid)
#     if age == "NaN" or isnan(age):
#         age = 30  # At this age, the mapping is nearest to the population average:  77kg for females and 89.8kg
#     if sex == "NaN" or isnan(sex):
#         w = (wbyage_male[int(age)] + wbyage_female[int(age)]) / 2
#     else:
#         w = wbyage_male[int(age)] if int(sex) == 1 else wbyage_female[int(age)]
#     weight[fixedpid] = float(w)
#     gender[fixedpid] = sex

# mongo = MongoClient(config.get("Data", "analysis"))
# db = mongo["PROSITC__std"]
# with sopen(config.get("Run", "lookup"), autopack=False, ondup="skip") as loo:
#     print("loop...")
#     for start_str in locker(gen(), dict__url=config.get("Run", "extralocationattrs"), timeout=180):  # 3min.
#         start = datetime.strptime(start_str, "%Y-%m-%d")
#         pids = db["all"].distinct("participantId", bydate(start=start, days=1, attribute="location"))
#         for pid in pids:
#             print("     ", pid)
#             docs = db["all"].aggregate([
#                 {"$match": bydate(start=start, days=1, attribute="location", participantId=pid)},
#                 {"$sort": {"measuredAt": 1}},
#                 {"$project": {"_id": False}}
#             ])
#             t0, lat0, lon0 = None, None, None
#             newdocs = []
#             for doc in docs:
#                 decoded = decode(doc["value"], loo, new=True, readonly=True)
#                 t, lat, lon = doc["measuredAt"], decoded['latitude'], decoded['longitude']
#                 if None in [lat, lon]:
#                     print("×", flush=True, end="")
#                     t0 = None
#                     continue
#                 if t0 is None:
#                     t0, lat0, lon0 = t, lat, lon
#                     continue
#                 d = geodesic([lat0, lon0], [lat, lon]).km
#                 dt = (t - t0).seconds / 3600
#                 if dt == 0:
#                     print("·", flush=True, end="")
#                     continue

#                 speed = d / dt  # km/h

#                 # CALORIES
#                 if pid not in weight:
#                     weight[pid] = (77 + 89.8) / 2
#                 gender_factor = 0.95
#                 if pid in gender:
#                     gender_factor = 0.9 if gender[pid] == 0 else 1
#                 if speed <= speed_cutoff * gender_factor:
#                     cal_per_min = speed * weight[pid] * 0.0175
#                     cals = cal_per_min * dt * 60  # cals burnt
#                     doc_cals = doc.copy()
#                     doc_cals["hash"] = "CA" + doc["hash"][2:]
#                     doc_cals["value"] = round(cals, 8)
#                     doc_cals["attribute"] = "CALORIES"
#                     newdocs.append(doc_cals)

#                 doc_d = doc.copy()
#                 doc_d["hash"] = "DI" + doc["hash"][2:]
#                 doc_d["value"] = round(d, 8)
#                 doc_d["attribute"] = "DISTANCE"
#                 newdocs.append(doc_d)

#                 doc_dt = doc.copy()
#                 doc_dt["hash"] = "DU" + doc["hash"][2:]
#                 doc_dt["value"] = round(dt, 8)
#                 doc_dt["attribute"] = "DURATION"
#                 newdocs.append(doc_dt)

#                 doc_speed = doc.copy()
#                 doc_speed["hash"] = "SP" + doc["hash"][2:]
#                 doc_speed["value"] = round(speed, 8)
#                 doc_speed["attribute"] = "SPEED"
#                 newdocs.append(doc_speed)
#             insert(db, "all", newdocs)

# """
# https://www.healthline.com/health/how-fast-can-a-human-run#top-speed
# fastest man 37km/h; woman 34km/h
# slowpaced walking (0.7 m s/s) through to moderate-paced running (5.0 m/s  = 18km/h ) - says a paper
# """
