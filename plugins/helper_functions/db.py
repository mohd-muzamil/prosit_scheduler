import traceback

from pymongo import InsertOne, UpdateOne, ReplaceOne
from pymongo.errors import BulkWriteError


def insert(db, collection, lst, verbose=True, create_indexes=True, dryrun=False, move_brand_to_field=False, replace_doc=False):
    c = 0
    if verbose and isinstance(lst, list):
        op = "update" if move_brand_to_field else "insert"
        print(f"\n{op} many ({len(lst)}) into {db.name}.{collection}", flush=True, end="\t\t")
    try:
        # Create or ignore already created indexes.
        if create_indexes and not dryrun:
            db[collection].create_index("participantId")
            db[collection].create_index("attribute")
            # db[collection].create_index("measuredAt")
            # db[collection].create_index("uploadedAt")
            # db[collection].create_index("hash", unique=True)
            # db[collection].create_index("brand")

        requests = []
        for doc in lst:
            c += 1
            if move_brand_to_field:
                if "brand" not in doc:
                    requests.append(UpdateOne(
                        {"hash": doc["hash"]},
                        {"$set": {"participantId": doc["participantId"][1:], "brand": doc["participantId"][0]}}
                    ))
            elif replace_doc:
                requests.append(ReplaceOne({"hash": doc["hash"]}, doc))
            else:
                requests.append(InsertOne(doc))
        if not requests:
            return 0

        if dryrun:
            print("DRY RUN", end=" ")
            return c
        try:
            db[collection].bulk_write(requests, ordered=False)
        except BulkWriteError as bwe:
            # Ignores errors (documentation says all operations will be attempted even after some errors).
            # TODO: Ignore only 'duplicate key error'
            print(f"{bwe.details['nInserted']}/{len(lst)} H:{len(bwe.details['writeErrors'])} ", end="", flush=True)


    except Exception as e:
        print("__________________________")
        print(lst)
        print("**************************")
        print(e)
        print("++++++++++++++++++++++++++")
        print(str(e))
        print("--------------------------")
        traceback.print_exc()
        exit()

    return c
