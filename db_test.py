import psycopg2
from psycopg2.extras import RealDictCursor
from pytz import timezone 
from datetime import datetime
#.env vars loaded
import os
from os.path import join, dirname
from dotenv import load_dotenv
import ast
import uuid

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

ipfs_url = os.getenv("ipfs")
nats_urls = os.getenv("nats")
nats_urls = ast.literal_eval(nats_urls)

# pg_url = os.getenv("pghost")
# pgdb = os.getenv("pgdb")
# pgport = os.getenv("pgport")
# pguser = os.getenv("pguser")
# pgpassword = os.getenv("pgpassword")

pg_url='216.48.190.128'
pgdb='postgres'
pgport='26257'
pguser='root'
pgpassword=''

ack = False

try:
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(host=pg_url, database=pgdb, port=pgport, user=pguser, password=pgpassword)
    # connection = psycopg2.connect(host='216.48.182.5', database='postgres',port='5432',user='postgres',password='Happy@123')

    # Create a cursor object
    cursor=connection.cursor(cursor_factory=RealDictCursor)
    
except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

def dbpush_activities(act_out):
    print("PUSHING THE CONTENTS TO DB")
    # try:
           
    if act_out is not None:
        
        # generate a UUID
        act_uuid = uuid.uuid4()
        
        # to form the title
        data = act_out['metaData']['object']
        num_people = len([item for item in data if item['class'] == 'Person'])
        activities = list(set([item['activity'][0] for item in data]))
        activity_string = ' and '.join(activities)
        if num_people == 1:
            title = f"1 person {activity_string}"
        else:
            title = f"{num_people} people {activity_string}"
        
        # Create a new Activity
        cursor.execute("""
            INSERT INTO "Activities" (id, "tenantId", "batchId", "memberId", location, title, timestamp, score, "deviceId", "createdAt", "updatedAt")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
            """,
            ((str(act_uuid),), act_out['tenant_id'], act_out['batchid'], None, "Bangalore, India", title, act_out['timestamp'], 26.085652173913044, act_out['deviceid'])
        )

        # Get the ID of the new Activity
        activity_id = cursor.fetchone()['id']
        
        print("ACTIVITY INSERTED: ", activity_id)
        
        # generate a UUID
        img_uuid = uuid.uuid4()
        
        img_timestamp = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
        
        img_name = f"ACTIVITY_{img_timestamp}"
        
        # Create a new Image
        cursor.execute("""
            INSERT INTO "Images" (id, name, "timeStamp", uri, "activityId", "thumbnailId", "logId", "createdAt", "updatedAt")
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
            """,
            ((str(img_uuid),), img_name, act_out['timestamp'], act_out['metaData']['cid'], activity_id, None, None)
        )  
        
        # Get the ID of the new Activity
        imgAct_id = cursor.fetchone()['id']
        
        print("IMAGE ACTIVITY DATA INSERTED: ", imgAct_id)
        
        if activity_id is not None:
            if act_out["metaData"]["detect"] > 0:
                
                # generate a UUID
                meta_uuid = uuid.uuid4()
                
                if act_out["type"] == "anomaly":
                    anomaly_type = "ANOMALY"
                else:
                    anomaly_type = None
                
                # Create a new Activity Meta
                cursor.execute("""
                    INSERT INTO "ActivityMeta" (id, "peopleCount", "vehicleCount", anomaly, "activityId", category, "createdAt", "updatedAt")
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
                    """,
                    ((str(meta_uuid),), act_out["metaData"]["count"]["peopleCount"], act_out["metaData"]["count"]["vehicleCount"], anomaly_type, activity_id, "DETECTION")
                )
                
                # Get the ID of the new Activity
                actMeta_id = cursor.fetchone()['id']
                
                print("ACTIVITY META DATA INSERTED: ", actMeta_id)
                
                if act_out["metaData"]["object"] is not None:
                    for item in act_out["metaData"]["object"]:
                        
                        # generate a UUID
                        log_uuid = uuid.uuid4()
                        
                        memDID = None
                        # memDID = "00de7f86-a34b-4aa5-9315-7da556783de0" # must change it later just a temporary variable
                        
                        if memDID is not None: # item["memID"] should be changed
                            # member_id = item["memDID"]
                            member_id = memDID    
                        else:
                            member_id = None
                            
                        # Execute the SELECT query to check if the member id exists
                        cursor.execute("""SELECT id FROM "Member" WHERE id = %s""", (member_id,))
                        
                        # Fetch the result
                        result = cursor.fetchone()
                        
                        # # Check if the result is not None (i.e., the member id exists)
                        # if result is not None:
                        #     print("Member ID exists in the Member table.")
                        # else:
                        #     print("Member ID does not exist in the Member table.")
                        #     # Create a new Activity Log
                        #     cursor.execute("""
                        #         INSERT INTO "Member" (id, type, "tenantId", track, "isUser", "faceid", "createdAt", "updatedAt")
                        #         VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
                        #         """,
                        #         (member_id, item["type"], act_out['tenantid'], item["track"], False, item["face_cid"])
                        #     )
                            
                        # Create a new Activity Log
                        cursor.execute("""
                            INSERT INTO "Logs" (id, "_id", class, track, activity, cid, "memberId", "activityId", "createdAt", "updatedAt")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
                            """,
                            ((str(log_uuid),), item["id"], item["class"], item["track"], (item["activity"])[0], item["cids"], member_id, activity_id)
                        )
                        
                        # Get the ID of the new Activity
                        log_id = cursor.fetchone()['id']
                        
                        print("ACTIVITY LOG DATA INSERTED: ", log_id) 
                        
                        # generate a UUID
                        img_uuid = uuid.uuid4()
                        
                        img_timestamp = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
                        
                        img_name = f"ACTIVITY_{img_timestamp}"
                        
                        # Create a new Image
                        cursor.execute("""
                            INSERT INTO "Images" (id, name, "timeStamp", uri, "activityId", "thumbnailId", "logId", "createdAt", "updatedAt")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
                            """,
                            ((str(img_uuid),), img_name, item["detectTime"], item["cids"], None, None, log_id)
                        )  
                        
                        # Get the ID of the new Activity
                        imgLog_id = cursor.fetchone()['id']
                        
                        print("IMAGE LOG DATA INSERTED: ", imgLog_id)

        connection.commit()
        
        return ("Inserted an Activity")
    
    # except (Exception, psycopg2.Error) as error:
    #     print("Error while fetching data from PostgreSQL", error)
        
    #     # closing database connection.
    #     if connection:
    #         cursor.close()
    #         connection.close()
    #         print("PostgreSQL connection is closed")
        
    # finally:
    #     # closing database connection.
    #     if connection:
    #         cursor.close()
    #         connection.close()
    #         print("PostgreSQL connection is closed")


# activity_out = {'type': 'activity', 'deviceid': '40a33680-e95a-11ed-9138-29fd70751e73', 'batchid': '7fdba220-b1b7-4da2-b35a-e7cc74e7dab4', 'timestamp': '2023-05-02 16:21:14.384387', 'geo': {'latitude': 12.918747, 'longitude': 77.431558}, 'metaData': {'detect': 2, 'frameAnomalyScore': [0, 0, 25.12, 25.79, 26.95, 27.329999999999995, 26.660000000000004, 25.51, 24.04, 0, 0, 0, 24.29, 30.160000000000004, 29.339999999999996, 27.625, 28.7, 27.610000000000003, 28.1, 31.5, 30.76, 29.464999999999996, 27.329999999999995, 26.419999999999998, 27.43, 29.080000000000002, 32.75, 33.25, 29.920000000000005, 28.080000000000002], 'count': {'peopleCount': 2, 'vehicleCount': 0, 'ObjectCount': 0}, 'anamolyIds': [], 'cid': 'QmXbCZHPHeuYhwv4JWrGXzkCTLfa4H6aSqykfK52pvLnqu', 'object': [{'class': 'Person', 'detectionScore': 29.899411764705885, 'activityScore': 10.0, 'track': '100', 'id': '4.0', 'activity': ['walking'], 'detectTime': '', 'cids': 'Qmd6EZKo743ciyAg3dndmCUp3a7HxCBVDJvM9cHwF69cvk'}, {'class': 'Person', 'detectionScore': 27.54181818181818, 'activityScore': 10.0, 'track': '100', 'id': '5.0', 'activity': ['walking', 'carrying'], 'detectTime': '', 'cids': 'QmSxBtCBcpLvfeqWfSTR9ydR6TDZCRVRZLNAGdeG75D4J7'}]}, 'memory': '4.663916015625 GB', 'version': 'v0.0.3', 'tenantid': 'a7c4c832-249b-4a81-93da-21f56708f484'}
        
# dbpush_activities(activity_out)

# print(activity_out['timestamp'])



# for key in (activity_out["metaData"]):
#     print(key)

