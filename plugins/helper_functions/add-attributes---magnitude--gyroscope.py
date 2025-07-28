# from datetime import datetime, timedelta
# from math import sqrt
# from pprint import pprint

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


# mongo = MongoClient(config.get("Data", "analysis"))
# db = mongo["PROSITC__std"]
# with sopen(config.get("Run", "lookup"), autopack=False, ondup="skip") as loo:
#     print("loop...")
#     for start_str in locker(gen(), dict__url=config.get("Run", "maggyroscope"), timeout=180):  # 3min.
#         start = datetime.strptime(start_str, "%Y-%m-%d")
#         pids = db["all"].distinct("participantId", bydate(start=start, days=1, attribute={"$in": ["gyroscope_rad_s__x_y_z", "Gyroscope"]}))
#         for pid in pids:
#             print("     ", pid)
#             docs = db["all"].aggregate([
#                 {"$match": bydate(start=start, days=1, attribute={"$in": ["gyroscope_rad_s__x_y_z", "Gyroscope"]}, participantId=pid)},
#                 {"$sort": {"measuredAt": 1}},
#                 {"$project": {"_id": False}}
#             ])

#             newdocs = []
#             for doc in docs:
#                 v = doc["value"]
#                 try:
#                     mag = sqrt(sum(float(x) ** 2 for x in v))
#                 except Exception as e:
#                     print(e)
#                     pprint(doc)
#                     exit()


#                 doc_gyr = doc.copy()
#                 doc_gyr["hash"] = "GY" + doc["hash"][2:]
#                 doc_gyr["value"] = round(mag, 8)
#                 doc_gyr["attribute"] = "GYRO_MAG"
#                 newdocs.append(doc_gyr)

#             insert(db, "all", newdocs)
