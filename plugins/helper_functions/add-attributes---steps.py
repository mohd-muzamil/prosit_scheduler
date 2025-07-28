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


# mongo = MongoClient(config.get("Data", "analysis"))
# db = mongo["PROSITC__std"]
# with sopen(config.get("Run", "lookup"), autopack=False, ondup="skip") as loo:
#     print("loop...")
#     for start_str in locker(gen(), dict__url=config.get("Run", "stepsraw"), timeout=180):  # 3min.
#         start = datetime.strptime(start_str, "%Y-%m-%d")
#         pids = db["all"].distinct("participantId", bydate(start=start, days=1, attribute="Steps"))
#         for pid in pids:
#             print("     ", pid)
#             docs = db["all"].aggregate([
#                 {"$match": bydate(start=start, days=1, attribute="Steps", participantId=pid)},
#                 {"$sort": {"measuredAt": 1}},
#                 {"$project": {"_id": False}}
#             ])

#             newdocs = []
#             for doc in docs:
#                 v = doc["value"]
#                 steps = float(v[2])
#                 start_t = v[0]
#                 stop_t = v[1]
#                 fmt = "%Y-%m-%d %H:%M:%S"
#                 if "AM" in start_t + stop_t or "PM" in start_t + stop_t:
#                     fmt = fmt.replace("%H", "%I") +  " %p"
#                 try:
#                     secs = (datetime.strptime(stop_t.split(" +")[0], fmt) - datetime.strptime(start_t.split(" +")[0], fmt)).seconds
#                 except Exception as e:
#                     print(fmt)
#                     print(stop_t)
#                     print(start_t)
#                     print(e)
#                     exit()
#                 if secs == 0:
#                     print("×", flush=True, end="")
#                     continue

#                 doc_steps = doc.copy()
#                 doc_steps["hash"] = "ST" + doc["hash"][2:]
#                 doc_steps["value"] = round(steps, 8)
#                 doc_steps["attribute"] = "STEPS"
#                 newdocs.append(doc_steps)

#             insert(db, "all", newdocs)
