# from datetime import datetime, timedelta
# from math import sqrt

# from dateutil.utils import today
# from pymongo import MongoClient
# from shelchemy.cache import sopen
# from shelchemy.locker import locker

# from server.config import config
# from server.db import insert
# from server.tools import bydate


# def gen():
#     start_ = datetime.strptime("2019-10-01", "%Y-%m-%d")
#     while start_ < today():
#         yield str(start_).split(" ")[0]
#         start_ += timedelta(days=1)


# attributes = {"$in": ["accelerometer_m_s2__x_y_z", "Accelerometer", "magnetometer_muT__x_y_z", "Magnetometer"]}
# mongo = MongoClient(config.get("Data", "analysis"))
# db = mongo["PROSITC__std"]
# with sopen(config.get("Run", "lookup"), autopack=False, ondup="skip") as loo:
#     print("loop...")
#     for start_str in locker(gen(), dict__url=config.get("Run", "accmagnet"), timeout=180):  # 3min.
#         start = datetime.strptime(start_str, "%Y-%m-%d")
#         pids = db["all"].distinct("participantId", bydate(start=start, days=1, attribute=attributes))
#         for pid in pids:
#             print("     ", pid)
#             docs = db["all"].aggregate([
#                 {"$match": bydate(start=start, days=1, attribute=attributes, participantId=pid)},
#                 {"$sort": {"measuredAt": 1}},
#                 {"$project": {"_id": False}}
#             ])
#             t0 = None
#             newdocs = []
#             for doc in docs:
#                 a = doc["attribute"]
#                 t = doc["measuredAt"]
#                 x, y, z = [float(v) for v in doc["value"]]
#                 if t0 is None:
#                     t0, x0, y0, z0 = t, x, y, z
#                     continue
#                 d = sqrt((x - x0) ** 2 + (y - y0) ** 2 + (z - z0) ** 2)

#                 doc_d = doc.copy()
#                 doc_d["value"] = round(d, 8)
#                 if a in ["accelerometer_m_s2__x_y_z", "Accelerometer"]:
#                     doc_d["hash"] = "AD" + doc["hash"][2:]
#                     doc_d["attribute"] = "ACCEL_D"
#                 elif a in ["magnetometer_muT__x_y_z", "Magnetometer"]:
#                     doc_d["hash"] = "MD" + doc["hash"][2:]
#                     doc_d["attribute"] = "MAGNET_D"

#                 newdocs.append(doc_d)
#             insert(db, "all", newdocs)


