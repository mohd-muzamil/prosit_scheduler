# from datetime import datetime, timedelta

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


# """ Useful numeric attributes:
# "Brightness", "light_lux", 
# "soundPressureLevel_dB", "Sleep_Noise"
# """

# attributes = {"$in": ["detectedActivityConfidence__inVehicle_onBicycle_onFoot_running_still_tilting_unknown_walking", "Activity"]}
# mongo = MongoClient(config.get("Data", "analysis"))
# db = mongo["PROSITC__std"]
# with sopen(config.get("Run", "lookup"), autopack=False, ondup="skip") as loo:
#     print("loop...")
#     for start_str in locker(gen(), dict__url=config.get("Run", "activityauto"), timeout=120):  # 2min.
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
#                 foot, tilting, still, walking, running, vehicle, bike, unknown = None, None, None, None, None, None, None, None
#                 if a == "detectedActivityConfidence__inVehicle_onBicycle_onFoot_running_still_tilting_unknown_walking":
#                     l = ["vehicle", "bike", "foot", "running", "still", "tilting", "unknown", "walking"]
#                     vs = {k: v for v, k in reversed(sorted((float(v), k) for k, v in zip(l, doc["value"]) if k != "foot"))}
#                 elif a == "Activity":
#                     l = ["still", "walking", "running", "vehicle", "bike", "unknown"]
#                     items = doc["value"][0].split(", ")
#                     confidence = 100 if items[13] == "High" else 50
#                     vs = {k: v for v, k in reversed(sorted((confidence * float(v), k) for k, v in zip(l, items[1::2])))}

#                 act, v1 = list(vs.items())[0]
#                 # if act == "unknown":
#                 #     print("×", flush=True, end="")
#                 #     continue
#                 v2 = list(vs.values())[1]
#                 wr = ["running", "walking"]
#                 if v1 in wr and v2 in wr:
#                     # walking and running are similar, resort to the third place
#                     v2 = list(vs.values())[2]
#                 margin = v1 - v2
#                 # if margin < 1:
#                 #     print("<", flush=True, end="")
#                 #     continue

#                 doc_d = doc.copy()
#                 doc_d["value"] = round(margin, 8)
#                 doc_d["hash"] = "AM" + act[:2] + doc["hash"][4:]
#                 doc_d["attribute"] = f"ACTIVITY_M_{act}"

#                 newdocs.append(doc_d)
#             insert(db, "all", newdocs)

# """
# detectedActivityConfidence__inVehicle_onBicycle_onFoot_running_still_tilting_unknown_walking xxxxxxxxx 
# [0, 0, 0, 0, 100, 0, 0, 0]

# Activity xxxxxxxxx 
# ['isStationary, 0, isWalking, 0, isRunning, 0, isInVehicle, 0, isCycling, 0, isActivityUnknown, 1, Confidence, High']
# Activity xxxxxxxxx 
# ['isStationary, 1, isWalking, 0, isRunning, 0, isInVehicle, 0, isCycling, 0, isActivityUnknown, 0, Confidence, Low']            



# https://blog.mindorks.com/activity-recognition-in-android-still-walking-running-driving-and-much-more/

# STILL: When the mobile device will be still i.e. the user is either sitting at some place or the mobile device is having no motion
# ON_FOOT: When the mobile device is moving at a normal speed i.e. the user carrying the mobile device is either walking or running
# WALKING: This is a sub-activity of the ON_FOOT
# RUNNING: This is also a sub-activity of ON_FOOT
# IN_VEHICLE: When the mobile device is in the bus or car or some other kind of vehicle or the user holding the mobile device is present in the vehicle
# ON_BICYCLE: When the device is on the bicycle or the user carrying the mobile is on a bicycle
# TILTING: When the mobile device is being lifted and is having some angle with the flat surface
# UNKNOWN: When the device is unable to detect any activity on the mobile device
# """
