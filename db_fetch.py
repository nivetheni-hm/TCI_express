import psycopg2
from pytz import timezone 
from datetime import datetime
#.env vars loaded
import os
from os.path import join, dirname
from dotenv import load_dotenv
import ast

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

ipfs_url = os.getenv("ipfs")
nats_urls = os.getenv("nats")
nats_urls = ast.literal_eval(nats_urls)

pg_url = os.getenv("pghost")
pgdb = os.getenv("pgdb")
pgport = os.getenv("pgport")
pguser = os.getenv("pguser")
pgpassword = os.getenv("pgpassword")


ack = False



def fetch_db():
    try:
        device_info = {}
        
        connection = psycopg2.connect(host=pg_url, database=pgdb, port=pgport, user=pguser, password=pgpassword)
        # connection = psycopg2.connect(host='216.48.182.5', database='postgres',port='5432',user='postgres',password='Happy@123')

        cursor=connection.cursor()
        
        # query='''select dev.id,metadev.urn,metadev.ddns,metadev.ip,metadev.port,metadev."videoEncodingInformation",dev."remoteUsername",metadev.rtsp,dev."remoteDeviceSalt",feature."name",ge.latitude,ge.longitude
        #     from "Device" dev
        #     inner join "DeviceMetaData" metadev on dev."deviceId"= metadev."deviceId"
        #     inner join "Feature" feature on dev."deviceId"= feature."deviceId"
        #     inner join "Geo" ge on  ge."deviceMetaDataId" = metadev.id
        #     ;'''
        
        # '''SELECT dev.id, dev."tenantId", metadev.urn, metadev.ddns, metadev.ip, metadev.port, metadev."videoEncodingInformation", dev."remoteUsername", metadev.rtsp, dev."remoteDeviceSalt", feat."name", ge.latitude, ge.longitude
        #         FROM "Device" dev
        #         INNER JOIN "DeviceMetaData" metadev ON dev."deviceId" = metadev."deviceId"
        #         INNER JOIN "DeviceFeatures" devfeat ON dev."deviceId" = devfeat."deviceId" AND devfeat.enabled = True
        #         INNER JOIN "Features" feat ON devfeat."featureId" = feat.id
        #         INNER JOIN "Geo" ge ON ge."deviceMetaDataId" = metadev.id;
        #     ;'''

        query =  '''SELECT dev.id, dev."tenantId", metadev.urn, metadev.ddns, metadev.ip, metadev.port, metadev."videoEncodingInformation", dev."remoteUsername", metadev.rtsp, dev."remoteDeviceSalt", ARRAY_AGG(feat."name") AS feature_names, ge.latitude, ge.longitude
                FROM "Device" dev
                INNER JOIN "DeviceMetaData" metadev ON dev."deviceId" = metadev."deviceId"
                INNER JOIN "DeviceFeatures" devfeat ON dev."deviceId" = devfeat."deviceId" AND devfeat.enabled = True
                INNER JOIN "Features" feat ON devfeat."featureId" = feat.id
                INNER JOIN "Geo" ge ON ge."deviceMetaDataId" = metadev.id
                GROUP BY dev.id, dev."tenantId", metadev.urn, metadev.ddns, metadev.ip, metadev.port, metadev."videoEncodingInformation", dev."remoteUsername", metadev.rtsp, dev."remoteDeviceSalt", ge.latitude, ge.longitude;
            ;'''
            
        cursor.execute(query)
        
        print("Selecting rows from device table using cursor.fetchall")
        device_records = cursor.fetchall()
        return(device_records)
        # for row in device_records:
        #     # print(row)
        #     device_info[str(row[0])] = {}
        #     device_info[str(row[0])]["tenantId"] = row[1]
        #     device_info[str(row[0])]["urn"] = row[2]
        #     device_info[str(row[0])]["ip"] = row[4]
        #     device_info[str(row[0])]["rtsp"] = row[8]
        #     device_info[str(row[0])]["username"] = row[7]
        #     device_info[str(row[0])]["password"] = row[9]
        #     device_info[str(row[0])]["ddns"] = row[3]
        #     device_info[str(row[0])]["videoEncodingInformation"] = row[6]
        #     device_info[str(row[0])]["port"] = int(row[5])
        #     device_info[str(row[0])]["subscriptions"] = row[10]
        #     device_info[str(row[0])]["lat"] = row[11]
        #     device_info[str(row[0])]["long"] = row[12]
    
        # return(device_info)
            
        # connection.commit()
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        
    # finally:
    #     # closing database connection.
    #     if connection:
    #         cursor.close()
    #         connection.close()
    #         print("PostgreSQL connection is closed")

# device_data = fetch_db()
# print(device_data)