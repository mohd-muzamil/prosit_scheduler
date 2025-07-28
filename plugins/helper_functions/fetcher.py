import os
from datetime import datetime, time
import pymongo
import base64
import hashlib
import sqlite3
import rncryptor as rn
import sqlite3
from helper_functions.config import config
import re


def decryptLocation(encoded, con, discard_address=True, update_lookup=False):
    '''
        Function to decrypt location points to get them in raw form.
    '''
    try:
        encoded2 = encoded.encode("latin-1")
        id = hashlib.md5(encoded2).hexdigest()
        # print(f"{encoded} \n {encoded2} \n {id}")
        query = "SELECT blob FROM content WHERE id='" + id + "';"
        res = con.execute(query)
        loc = res.fetchone()
        res.close()

        if loc is None:
            try:
                # Decryption in Python
                decrypted = rn.decrypt(base64.b64decode(encoded), 'dummy')

                # Update DB with decrypted text
                if update_lookup:
                    query_update = f"INSERT INTO content (id, blob) SELECT '{id}', '{decrypted}' WHERE NOT EXISTS (SELECT 1 FROM content WHERE id = '{id}')"
                    con.execute(query_update)
                    con.commit()

                loc = decrypted
                # print(f"location {loc} updated in lookup table")

            except Exception as e:
                # print(e)
                return None
        else:
            loc = loc[0]

        if isinstance(loc, bytes):
            ret = loc.decode('utf-8').split(" | ")
        else:
            ret = loc.split(" | ")
        # removing last value from address as this contains address information.
        if discard_address:
            ret = ret[:-1]
        ret = ', '.join(str(x) for x in ret)
        return ret
    
    except Exception as e:
        # Handle any exceptions that occur during the execution of the function
        print(f"An error occurred: {e}")
        return None
    

def fetch(study, first_day="2000-12-31", last_day="2023-06-01", attributes=[], brands=["a", "i", "x"], prositids=[], custom_filter={},
          skip=0, limit=None, sorting=False, include_prositid=True, include_attribute=True, include_brand=True,
          lookup_file="/home/sensitive/prosit-location-lookup.db", warn_nodata=True, discard_address=True):
    '''
        Function to fetch data from mongoDB based on the parameters passed to it.
    '''
    study = study.split("_")[0] + "__std"
    # login = os.getenv("LOGNAME")
    # with open(f"/home/{login}/mongodb.txt", "r") as f:
        # passw = f.read().strip()

    # connecting to mongodb database using valid credentials stored in a secret file.
    # url = f"mongodb://{login}:{passw}@localhost/admin?authSource=admin"
    url = config.get("Data", "analysis")
    client = pymongo.MongoClient(url)
    db = client[study]
    collection = db["all"]

    date_format = '%Y-%m-%d'
    # Now, extract only the date component
    iso_dt1 = datetime.strptime(first_day, date_format)
    iso_dt2 = datetime.strptime(last_day, date_format)
    
    # Construct the date query
    date = {
        "measuredAt": {
            "$gte": iso_dt1,
            "$lte": iso_dt2
        }
    }
    
    # Construct the filter in pymongo format
    filter = {
                "$match": {
                    **date
                }
            }
    
    if isinstance(brands, list) and len(brands) > 0:
        filter['$match']["brand"] = {
                            "$in": brands
                        }
    if isinstance(attributes, list) and len(attributes) > 0:
        filter['$match']["attribute"] = {
                            "$in": attributes
                        }
    if isinstance(prositids, list) and len(prositids) > 0:
        filter["$match"]["participantId"] = {
                            "$in": prositids
                        }
    
    # if isinstance(custom_filter, dict) and len(custom_filter) > 0:
        # filter["$match"].update(custom_filter)

    # Construct the project in pymongo format
    project = {
        "$project": {
            "_id": False,
            "participantId": True,
            "attribute": True,
            "value": True,
            "measuredAt": True,
            "brand": True,
        }
    }

    skip = {"$skip": int(skip)}

    # Check if the 'limit' parameter is None, and if so, fetch all records.
    if limit is None:
        # Set a large number to effectively fetch all records.
        limit = {"$limit": 100000}
    else:
        limit = {"$limit": int(limit)}

    sort = {
            "$sort": {
                "measuredAt": 1 if sorting else -1
            }
        }
    
    # Construct the pipeline in pymongo format
    pipeline = [filter, project, skip, limit]
    if isinstance(sorting, bool) and sorting:
        pipeline.append(sort)

    # Perform the aggregation query
    results = list(collection.aggregate(pipeline))
    if len(results) == 0:
        if warn_nodata:
            # print(f"No results found for study: '{study}', attributes: '{attributes}', prositids: '{prositids}', brands: '{brands}', last_day: '{last_day}'.")
            pass
        return []
    else:
        ##########################################################
        # check if attiribute if location and perform decryption before returnin the values.
        # for rest of the attribute, return the data as it is.
        if "location" in attributes:
            if not os.path.exists(lookup_file):
                raise ValueError(f"The path of the lookup file could not be found/read to decrypt location: lookup_file='{lookup_file}'")
            con = sqlite3.connect(lookup_file)

        for r in results:
            if r["attribute"] == "location":
                r["value"] = decryptLocation(r["value"], con, discard_address=discard_address, update_lookup=False)
                
            # elif r["attribute"] in ['Accelerometer',  'Activity',  'Analytics',  'Anxiety level',  'App_Use',  'BalloonGame',  'Brightness',  'Call',  'Device',  'EMA Questions',  'Energy_Question',  'FlankerGame',  'Gyroscope',  'HR',  'Heart Rate',  'Heart_Event',  'Heart_Rate',  'Heart_Rate_Variability',  'Imagine',  'Journal',  'Lock state',  'Magnetometer',  'Meditation',  'Mood_Question',  'Music',  'Noise',  'Photo Book',  'Power state',  'Reachability',  'Resting_Heart_Rate',  'Sleep',  'Sleep_Noise',  'Speaking out loud',  'Steps',  'Survey',  'Typing Metrics',  'Weather',  'accelerometer_m_s2__x_y_z',  'ambientTemperature_celsius',  'bluetooth__bluetoothClass_bondState_deviceAdress_deviceName_id_type',  'bluetooth__bluetoothClass_bondState_deviceAdress_deviceName_type',  'calls__callDate_callDurationS_callType_phoneNumberHash',  'connectivity',  'debug',  'detectedActivityConfidence__inVehicle_onBicycle_onFoot_running_still_tilting_unknown_walking',  'geomagneticRotationVector__cos_x_y_z',  'gyroscope_rad_s__x_y_z',  'images__future_negative_future_neutral_future_positive_stimulus',  'installedApps',  'light_lux', 'magnetometer_muT__x_y_z',  'moodRating',  'mood__mood_anxious_mood_happy_mood_sad_mood_stressed_mood_tired',  'notifications__action_flags_package',  'powerState',  'pressure_hPa',  'proximity_cm',  'pss4__mood_pss4_1_mood_pss4_2_mood_pss4_3_mood_pss4_4',  'questionnaire__PANASaengstlich_PANASaktiv_PANASbeschamt_PANASentschlossen_PANASfreudigerregt_PANASnervoes_PANASniedergeschlagen_PANASwach_Rumination_SelfEfficacy1_SelfEfficacy2_SocialSupport1_SocialSupport2_SocialSupport3_Worry1_Worry2',  'relativeHumidity_percent',  'rotationVector__cos_x_y_z',  'sensorAccuracy__accuracy_sensor',  'smile_app_user_tracking',  'sms__numberLetters_phoneNumberHash_smsDate_smsType',  'soundPressureLevel_dB',  'stepCounter_sinceLastReboot',  'survey__answer_question',  'systemInfo',  'usageEvents',  'useGoals',  'useSleep']:
            #     r["value"] = list(map(float, r["value"]))

            if not include_prositid:
                r.pop("participantId")
            if not include_attribute:
                r.pop("attribute")
            if not include_brand:
                r.pop("brand")

        return results
    

def fetch_distinct_participants(study):
    '''
    study: study name ex: 'SMILE'
    Return:participant_dict dict of participantId, device type
    '''
    study = study.split("_")[0] + "__std"

    # login = os.getenv("LOGNAME")
    # with open(f"/home/{login}/mongodb.txt", "r") as f:
        # passw = f.read().strip()

    # connecting to mongodb database using valid credentials stored in a secret file.
    # url = f"mongodb://{login}:{passw}@localhost/admin?authSource=admin"
    url = config.get("Data", "analysis")
    client = pymongo.MongoClient(url)
    db = client[study]
    collection = db["all"]
    participants = collection.distinct('participantId')

    return participants
