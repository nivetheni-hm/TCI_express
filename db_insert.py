import psycopg2
from psycopg2 import OperationalError, Error, DatabaseError
from psycopg2.extras import RealDictCursor
from pytz import timezone 
from datetime import datetime
#.env vars loaded
import os
from os.path import join, dirname
from dotenv import load_dotenv
import ast
import uuid
import reverse_geocode
import json

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

def dbpush_activities(act_out):

    try:
        print("PUSHING THE CONTENTS TO DB")
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(host=pg_url, database=pgdb, port=pgport, user=pguser, password=pgpassword)
        # Create a cursor object
        cursor=connection.cursor(cursor_factory=RealDictCursor)
        
        # Convert the object array to JSON string
        object_array = json.dumps(act_out['metaData']['object'])
            
        # to form the title
        data = act_out['metaData']['object']
        num_people = len([item for item in data if item['class'] == 'Person'])
        activities = list(set([item['activity'] for item in data]))
        activity_string = ' and '.join(activities)
        if num_people == 1:
            title = f"1 person {activity_string}"
        else:
            title = f"{num_people} people {activity_string}"
        
        # img_name = f"ACTIVITY_{act_out['timestamp']}"
        
        if act_out["type"] == "anomaly":
            anomaly_type = "ANOMALY"
        else:
            anomaly_type = None
        
        coordinates = [(act_out['geo']['latitude'], act_out['geo']['longitude'])]
        result = reverse_geocode.search(coordinates)
        loc_name = str(result[0]['city'])
        
        # img_name_1 = f"LOG_{act_out['timestamp']}"
        
        query = """
            WITH inserted_activity AS (
            INSERT INTO "Activities" (id, "tenantId", "batchId", "memberId", location, title, timestamp, score, "deviceId", "createdAt", "updatedAt")
            VALUES (uuid_generate_v4(), %(tenantId)s, %(batchId)s, %(memberId)s, %(location)s, %(title)s, %(timestamp)s, %(score)s, %(deviceId)s, NOW(), NOW())
            RETURNING id
            ),
            inserted_images AS (
            INSERT INTO "Images" (id, name, "timeStamp", uri, "tenantId", "activityId", "thumbnailId", "logId", "createdAt", "updatedAt")
            SELECT uuid_generate_v4(), 'ACTIVITY_' || to_char(NOW(), 'YYYYMMDDHH24MISSMS'), %(timestamp)s, %(uri1)s, %(tenantId)s, inserted_activity.id, %(thumbnailId)s, %(logId)s, NOW(), NOW()
            FROM inserted_activity
            RETURNING id
            ),
            inserted_activity_meta AS (
            INSERT INTO "ActivityMeta" (id, "peopleCount", "vehicleCount", anomaly, "activityId", category, "createdAt", "updatedAt")
            SELECT uuid_generate_v4(), %(peopleCount)s, %(vehicleCount)s, %(anomaly)s, inserted_activity.id, %(category)s, NOW(), NOW()
            FROM inserted_activity
            RETURNING id
            ),
            inserted_geo AS (
            INSERT INTO "Geo" (id, latitude, name, longitude, "deviceMetaDataId", "metaId", "createdAt", "updatedAt")
            SELECT uuid_generate_v4(), %(latitude)s, %(location)s, %(longitude)s, (SELECT d.id FROM "DeviceMetaData" d WHERE d."deviceId" = %(deviceId)s), inserted_activity_meta.id, NOW(), NOW()
            FROM inserted_activity_meta
            RETURNING id
            ),
            inserted_logs AS (
            INSERT INTO "Logs" (id, "tenantId", "_id", class, track, activity, cid, "memberId", "activityId", "createdAt", "updatedAt")
            SELECT uuid_generate_v4(), %(tenantId)s, object->>'id', object->>'class', object->>'track', object->>'activity', object->>'cids', %(memberId)s, inserted_activity.id, NOW(), NOW()
            FROM inserted_activity, jsonb_array_elements(%(objects)s) AS object
            RETURNING id
            ),
            inserted_images2 AS (
            INSERT INTO "Images" (id, name, "timeStamp", uri, "tenantId", "activityId", "thumbnailId", "logId", "createdAt", "updatedAt")
            SELECT uuid_generate_v4(), 'LOG_' || to_char(NOW(), 'YYYYMMDDHH24MISSMS'), %(timestamp)s, object->>'cids', %(tenantId)s, %(activityId)s, %(thumbnailId)s, inserted_logs.id, NOW(), NOW()
            FROM inserted_logs, jsonb_array_elements(%(objects)s) AS object
            RETURNING id
            )
            SELECT * 
            FROM (
                SELECT * FROM inserted_activity
                UNION ALL
                SELECT * FROM inserted_images
                UNION ALL
                SELECT * FROM inserted_activity_meta
                UNION ALL
                SELECT * FROM inserted_geo
                UNION ALL
                SELECT * FROM inserted_logs
                UNION ALL
                SELECT * FROM inserted_images2
            )AS inserted_rows;
        """
        
        cursor.execute(query,{
        'tenantId': act_out['tenantId'], 
        'batchId': act_out['batchid'], 
        'memberId': None, 
        'location': str(loc_name), 
        'title': title, 
        'timestamp': act_out['timestamp'], 
        'score': act_out['metaData']['frameAnomalyScore'], 
        'deviceId': act_out['deviceid'],
        'uri1': act_out['metaData']['cid'],
        'thumbnailId': None, 
        'logId': None, 
        'activityId': None,
        'peopleCount': act_out["metaData"]["count"]["peopleCount"], 
        'vehicleCount': act_out["metaData"]["count"]["vehicleCount"], 
        'anomaly': anomaly_type, 
        'category': "DETECTION", 
        'latitude': act_out['geo']['latitude'], 
        'longitude': act_out['geo']['longitude'],
        'objects': object_array
        })
        
        # Fetch the inserted activity_id and image_id
        # activity_id, image_id_1, activity_meta_id, geo_id, logs_id, image_id_2 = cursor.fetchone()
        
        # Fetch all inserted rows
        rows = cursor.fetchall()
        
        # # Get the affected row count
        # affected_rows = cursor.rowcount
        
        # print("COUNT: ",affected_rows)

        # Print the inserted rows
        for row in rows:
            print(row)
        
        # Commit the changes and close the connection
        connection.commit()
        cursor.close()
        connection.close()
        
        print("Data inserted successfully!")
        return("SUCCESS!!")
        # print("Activity ID:", activity_id)
        # print("Image ID:", image_id)
        
    except (Exception, psycopg2.Error, OperationalError, Error, DatabaseError) as error:
        # Handle exceptions and rollback the transaction if necessary
        if 'conn' in locals():
            connection.rollback()
            cursor.close()
            connection.close()
        print(f"Error occurred during data insertion: {error}")
        return("FAILURE!!")
        

# act_dict = {"type": "activity", "deviceid": "e99ecb59-878e-4c21-961f-6056c4e98b63", "batchid": "b530e737-79c4-4a17-a374-c1f101978c9a", "timestamp": "2023-05-12 12:23:58.050599", "geo": {"latitude": 26.25, "longitude": 88.11}, "metaData": {"detect": 1, "frameAnomalyScore": 5.0, "count": {"peopleCount": 1, "vehicleCount": 0, "ObjectCount": 0}, "anomalyIds": [], "cid": "QmRPZX5bn6h1VVUqfmVmthoMznMF2UsGqB4mwFDVAWEai5", "object": [{"class": "Person", "detectionScore": 5.0, "activityScore": 10.0, "track": "100", "id": "1", "memDID": "", "activity": "standing", "detectTime": "2023-05-12 12:23:55.729871", "cids": "QmNsfHCbJbxDJsqo6kKHem2xvM8atmPAsGz9sCKpgxqCst"}]}, "tenantId": "3d77252b-ba26-451a-8ad4-ee18159a6db8", "version": "v0.0.3"}
# dbpush_activities(act_dict)
