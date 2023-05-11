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
            INSERT INTO "Images" (id, name, "timeStamp", uri, "tenantId", "activityId", "thumbnailId", "logId", "createdAt", "updatedAt")
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
            """,
            ((str(img_uuid),), img_name, act_out['timestamp'], act_out['metaData']['cid'], act_out['tenant_id'], activity_id, None, None)
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
                            INSERT INTO "Logs" (id, "tenantId", "_id", class, track, activity, cid, "memberId", "activityId", "createdAt", "updatedAt")
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
                            INSERT INTO "Images" (id, name, "timeStamp", uri, "tenantId", "activityId", "thumbnailId", "logId", "createdAt", "updatedAt")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING id;
                            """,
                            ((str(img_uuid),), img_name, item["detectTime"], item["cids"], act_out['tenant_id'], None, None, log_id)
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


# activity_out = {'type': 'activity', 'deviceid': '793230b2-5dc9-41b4-ad38-57051e7bf26b', 'batchid': '6258b3c5-db85-4f3d-be72-eec08e5f1245', 'timestamp': '2023-05-09 19:09:14.817661', 'geo': {'latitude': 26.25, 'longitude': 88.11}, 'metaData': {'detect': 3, 'frameAnomalyScore': 5.0, 'count': {'peopleCount': 3, 'vehicleCount': 0, 'ObjectCount': 0}, 'anamolyIds': [], 'cid': 'QmXYHxYn5MYnszqKQ3iT4jEtPmf4ofGvNpY8vUjf8rrbqe', 'object': [{'class': 'Person', 'detectionScore': 5.0, 'activityScore': 10.0, 'track': '100', 'id': '19', 'memDID': '', 'activity': ['walking'], 'detectTime': '2023-05-09 19:09:13.551897', 'cids': 'QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH'}, {'class': 'Person', 'detectionScore': 5.0, 'activityScore': 10.0, 'track': '100', 'id': '13', 'memDID': '', 'activity': ['walking'], 'detectTime': '2023-05-09 19:09:13.551956', 'cids': 'QmbLJbwuobWPigypZ5pFDot5Yyu3YgAwyntex94TjY7NHC'}, {'class': 'Person', 'detectionScore': 5.0, 'activityScore': 10.0, 'track': '100', 'id': '20', 'memDID': '', 'activity': ['walking'], 'detectTime': '2023-05-09 19:09:13.551977', 'cids': 'QmQErEu43Z6QcQxjLxCXd8xWcSDURtcCZVg4n3b6t4piKA'}]}, 'tenant_id': '3d77252b-ba26-451a-8ad4-ee18159a6db8', 'version': 'v0.0.3'}
        
# dbpush_activities(activity_out)

# print(activity_out['timestamp'])



# for key in (activity_out["metaData"]):
#     print(key)

