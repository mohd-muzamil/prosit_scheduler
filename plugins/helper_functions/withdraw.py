# Description: Script to delete all the data belonging to a given participant from mongodb and PSQL database.
from time import sleep
from typing import Union


def withdraw(db, bkp_db, prositids: Union[str, list], start, end, dry_run=False, t=60, debug=True):
    """
    Remove all data from given participants.

    Ignore any time range provided at this Study object instantiation.
    Wait 't' seconds to ensure the user knows what they are doing.


    >>> withdraw("collection", t=2)
    WARNING: participants listed bellow will have all data erased from
    'MOCK' and backup in 2s ...
     ['collection']
    !
    WARNING: participants listed bellow will have all data erased from
    'MOCK' and backup in 1s ...
     ['collection']
    !
    Removing participant 'collection' from 'MOCK' ...
    <BLANKLINE>
    Removing collection 'collection'
    """
    if not isinstance(prositids, list):
        prositids = [prositids]
    if start != "2000-01-01" or end != None:
        print(f"Provided start ({start}) and end ({end}) dates will be ignored.")
    d = int(t / 4)
    while t > 0:
        print(f"WARNING: participants listed bellow will have all data erased from\n"
              f"'{db.name}' and backup in {t}s ...\n", prositids)
        print("!")
        sleep(d)
        t -= max(1, d)
        d = int(2 * d / 3)
    for part_name in prositids:
        print(f"Removing participant '{part_name}' from '{db.name}' ...")
        if part_name in db.list_collection_names():
            print(f"\nRemoving collection '{part_name}'")
            if not dry_run:
                db.drop_collection(part_name)
        for coll_name in bkp_db.list_collection_names():
            collection = bkp_db[coll_name]
            filter = {"participantId": part_name}
            if debug:
                print(f"\tFinding matches for {part_name} in {coll_name} ...")
            if docs := list(collection.find(filter)):
                print(f"\tRemoving {len(docs):8} matching documents in collection\t'{coll_name}' ...")
                if not dry_run:
                    collection.delete_many(filter)
