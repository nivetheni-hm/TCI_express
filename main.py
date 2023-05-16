import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib
import asyncio
import numpy as np
from datetime import datetime
from pytz import timezone
import os
from nats.aio.client import Client as NATS
import nats
from os.path import join, dirname
from dotenv import load_dotenv
import ast
import torch
import multiprocessing
from multiprocessing import Process, Queue, Pool
import uuid
import threading
import psycopg2
import shutil

# importing required functions
from db_fetch import fetch_db #to fetch data from postgres
from yolo_slowfast.deep_sort.deep_sort import DeepSort # import Deepsort tracking model
from anamoly_track import trackmain # model inference part
from dev import device_details
from db_push import gif_push, gst_hls_push
from lmdb_list_gen import attendance_lmdb_known, attendance_lmdb_unknown
from db_fetch_members import fetch_db_mem
from facedatainsert_lmdb import add_member_to_lmdb

Gst.init(None) # Initializes Gstreamer, it's variables, paths
nc_client = NATS() # global Nats declaration

# define constants and variables
frame_skip = {}
gif_batch = {}
pipelines = []
# device_details = []
known_whitelist_faces = []
known_whitelist_id = []
known_blacklist_faces = []
known_blacklist_id = []

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

# creation of directories for file storage
hls_path = "./Hls_output"
if os.path.exists(hls_path) is False:
    os.mkdir(hls_path)
    
gif_path = "./Gif_output"
if os.path.exists(gif_path) is False:
    os.mkdir(gif_path)
    
obj_model = torch.hub.load('Detection', 'custom', path='./best_yolov5.pt', source='local',force_reload=True)

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(host=pg_url, database=pgdb, port=pgport, user=pguser, password=pgpassword)
# Create a cursor object
cursor=connection.cursor()
def activity_trackCall(source, device_id, device_timestamp, device_data, datainfo, track_obj):

    queue1 = Queue()
    batchId = uuid.uuid4()
    
    trackmain(
        source,
        device_data,
        device_id, 
        batchId,
        queue1,
        datainfo,
        obj_model,
        track_obj,
        cursor
        )

def numpy_creation(img_arr, device_id, device_timestamp, device_info, track_obj, skip_dict, gif_dict):
        
    path = video_name_gif + '/' + 'camera.gif'
        
        # if(skip_dict[device_id] > 100):
        
        # filename for gif
    video_name_gif = gif_path + '/' + str(device_id)
    if not os.path.exists(video_name_gif):
        os.makedirs(video_name_gif, exist_ok=True)
        
    path = video_name_gif + '/' + 'camera.gif'
        
    if(skip_dict[device_id] < 30):
        gif_dict[device_id].append(img_arr)
    elif(skip_dict[device_id] == 31):
        threading.Thread(target=gif_push,args=(connection, cursor, path, device_info, gif_dict[device_id]),).start()
            
        
    if skip_dict[device_id] % 4 == 0:
        datainfo = [known_whitelist_faces, known_blacklist_faces, known_whitelist_id, known_blacklist_id]
        # activity_trackCall(img_arr, device_id, device_timestamp, device_info, datainfo, track_obj)
        threading.Thread(target=activity_trackCall,args=(img_arr, device_id, device_timestamp, device_info, datainfo, track_obj,)).start()

class PipelineWatcher:
    def __init__(self, pipelines):
        self.bus = None
        self.pipelines = pipelines
        self.loop = GLib.MainLoop()
        self.watch_buses()

    def watch_buses(self):
        for pipeline in self.pipelines:
            self.bus = pipeline.get_bus()
            self.bus.add_signal_watch()
            self.bus.connect("message", self.on_message, pipeline)

    def on_message(self, bus, message, pipeline):
        t = message.type
        if t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            src_element = message.src
            print(f"Error: {err}", debug)
            print("Error from: ", src_element.get_name())
            self.restart_pipeline(pipeline, src_element)
            # # Stop the pipeline before restarting
            # pipeline.set_state(Gst.State.NULL)
            # pipeline.set_state(Gst.State.READY)
            # pipeline.set_state(Gst.State.PLAYING)
            # # self.loop.quit()
        elif t == Gst.MessageType.WARNING:
            err, debug = message.parse_warning()
            print(f"Warning: {err}", debug)
        elif t == Gst.MessageType.EOS:
            src_element = message.src
            print("End of stream - Restarting pipeline: ", src_element.get_name())
            self.restart_pipeline(pipeline, src_element)
            # # Stop the pipeline before restarting
            # pipeline.set_state(Gst.State.NULL)
            # pipeline.set_state(Gst.State.READY)
            # pipeline.set_state(Gst.State.PLAYING)
            # # self.loop.quit()
        elif t == Gst.MessageType.STATE_CHANGED:
            old_state, new_state, pending_state = message.parse_state_changed()
            print(f"State changed: {old_state} -> {new_state}")
        # elif t == Gst.MessageType.STREAM_START:
        #     print("Stream started")
        # elif t == Gst.MessageType.STREAM_STATUS:
        #     status_type, owner = message.parse_stream_status()
        #     print(f"Stream status: {status_type} - {owner}")
        # elif t == Gst.MessageType.TAG:
        #     taglist = message.parse_tag()
        #     print(f"Tag: {taglist}")
        elif t == Gst.MessageType.ASYNC_DONE:
            print("Async done")
        # else:
        #     print(f"Unknown message: {t}")
        
    def restart_pipeline(self, pipeline, src_element):
        print("SOURCE ELEMENT: ", src_element.get_name())
        pipeline.set_state(Gst.State.NULL)
        pipeline.set_state(Gst.State.READY)
        pipeline.set_state(Gst.State.PLAYING)
        
    @staticmethod
    def on_new_sample(appsink, deviceId, deviceInfo, trackObj, frameSkip, gif_batch): # to fetch frames from appsink
        
        # print("Got a sample")
        sample = appsink.emit("pull-sample")
        buffer = sample.get_buffer()
        caps = sample.get_caps()
        width = caps.get_structure(0).get_value("width")
        height = caps.get_structure(0).get_value("height")
        nparray = np.ndarray(
            (height, width, 3),
            buffer=buffer.extract_dup(0, buffer.get_size()),
            dtype=np.uint8,
        )
        # # filename for hls
        # frame_name = './Frame_output' + '/' + str(deviceId)
        # if not os.path.exists(frame_name):
        #     os.makedirs(frame_name, exist_ok=True)
        
        # file_name = f'./{frame_name}/{frameSkip[deviceId]}.jpg'
        # cv2.imwrite(file_name, nparray)
        # print(nparray.shape)
        frameSkip[deviceId] += 1
        # # print("DEVICE ID: ", deviceId, "FRAME SKIP: ", frameSkip)
        print(f"Received frame with shape {nparray.shape}")
        datetime_ist = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
        numpy_creation(img_arr=nparray, device_id=deviceId, device_timestamp=datetime_ist , device_info=deviceInfo, track_obj=trackObj, skip_dict=frameSkip, gif_dict=gif_batch)
        return Gst.FlowReturn.OK

def gst_launcher(device_data, frame_skip, gif_dict):
    
    print("Entering Framewise and HLS Stream")
    
    track_obj = DeepSort("./yolo_slowfast/deep_sort/deep_sort/deep/checkpoint/ckpt.t7")
    
    # print("DEVICE ID: ", device_id)
    # print("DEVICE INFO: ", device_info)
    
    device_id = device_data['deviceId']
    location = device_data['rtsp'] # Fetching device info
    username = device_data['username']
    password = device_data['password']
    subscription = device_data['subscriptions']
    encode_type = device_data['videoEncodingInformation']
    ddns_name = device_data['ddns']
    urn = device_data['urn']
    lat = device_data['lat']
    long = device_data['long']
    
    # filename for hls
    video_name_hls = hls_path + '/' + str(device_id)
    if not os.path.exists(video_name_hls):
        os.makedirs(video_name_hls, exist_ok=True)
    print(video_name_hls)
    
    if(ddns_name == None):
        hostname = 'hls.ckdr.co.in'
    else:
        hostname = ddns_name
    
    # retry=5 retry-delay=1000 tcpserversink 
        
    if((encode_type.lower()) == "h264"):
        # pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=True ! rtph264depay name=g_depay_{device_id} ! h264parse name=g_parse_{device_id} ! avdec_h264 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=200'
        pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! tee name=g_t_{device_id} ! queue name=g_q_{device_id} ! rtph264depay name=g_depay_{device_id} ! h264parse name=g_parse_{device_id} ! avdec_h264 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=1 g_t_{device_id}. ! queue name=h_q_{device_id} ! rtph264depay name=h_depay_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}'
    if((encode_type.lower()) == "h265"):
        # pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=True ! rtph265depay name=g_depay_{device_id} ! h265parse name=g_parse_{device_id} ! avdec_h265 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=200'
        pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! tee name=g_t_{device_id} ! queue name=g_q_{device_id} ! rtph265depay name=g_depay_{device_id} ! h265parse name=g_parse_{device_id} ! avdec_h265 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=1 g_t_{device_id}. ! queue name=h_q_{device_id} ! rtph265depay name=h_depay_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}'    
    # if((encode_type.lower()) == "mp4"):
    #     pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" latency=50 timeout=300 drop-on-latency=true tee name=g_t_{device_id} ! queue name=g_q_{device_id} ! decodebin name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=10 g_t_{device_id}. ! queue name=h_q_{device_id} ! x264enc name=h_enc_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}'
    
    pipeline = Gst.parse_launch(pipeline_str)
    
    if((encode_type.lower()) == "h265" or (encode_type.lower()) == "h264"):
        # sink params
        sink = pipeline.get_by_name(f'h_sink_{device_id}')

        # Location of the playlist to write
        sink.set_property('playlist-root', f'https://{hostname}/live/stream{device_id}')
        # Location of the playlist to write
        sink.set_property('playlist-location', f'{video_name_hls}/{device_id}.m3u8')
        # Location of the file to write
        sink.set_property('location', f'{video_name_hls}/segment.%01d.ts')
        # The target duration in seconds of a segment/file. (0 - disabled, useful for management of segment duration by the streaming server)
        sink.set_property('target-duration', 10)
        # Length of HLS playlist. To allow players to conform to section 6.3.3 of the HLS specification, this should be at least 3. If set to 0, the playlist will be infinite.
        sink.set_property('playlist-length', 3)
        # Maximum number of files to keep on disk. Once the maximum is reached,old files start to be deleted to make room for new ones.
        sink.set_property('max-files', 6)
    
        if not sink or not pipeline:
            print("Not all elements could be created.")
        else:
            print("All elements are created and launched sucessfully!")
        
    pipeline.set_state(Gst.State.PLAYING)
    appsink = pipeline.get_by_name(f"g_sink_{device_id}")
    appsink.connect("new-sample", PipelineWatcher.on_new_sample, device_id, device_data, track_obj, frame_skip, gif_dict)
    pipelines.append(pipeline)
    
def run_pipeline(devices_for_process):
    
    global frame_skip
    print("LENGTH OF THE DEVICES IN A PROCESS: ", len(devices_for_process))
    # print(devices_for_process)

    for device_tuple in devices_for_process:
        device_dict = {
            'deviceId': device_tuple[0],
            # 'deviceId': devices_for_process.index(device_tuple),
            'tenantId': device_tuple[1],
            'urn': device_tuple[2],
            'ddns': device_tuple[3],
            'ip': device_tuple[4],
            'port': device_tuple[5],
            'videoEncodingInformation': device_tuple[6],
            'username': device_tuple[7],
            'rtsp': device_tuple[8],
            'password': device_tuple[9],
            'subscriptions': device_tuple[10],
            'lat': device_tuple[11],
            'long': device_tuple[12]
        }
        frame_skip[device_dict['deviceId']] = 0
        gif_batch[device_dict['deviceId']] = []
        gst_launcher(device_dict, frame_skip, gif_batch)
        threading.Thread(target=gst_hls_push,args=(connection, cursor, device_dict),).start()
        
    GLib.MainLoop().run()

def call_gstreamer(device_details): # iterate through the device list and start the gstreamer pipeline
    print("Got device info from DB")
    # Calculate the number of devices per process
    devices_per_process = 5
    num_processes = (len(device_details) - 1) // devices_per_process + 1
    # Start multiple processes and distribute the devices across them
    for i in range(num_processes):
        start_index = i * devices_per_process
        end_index = min((i + 1) * devices_per_process, len(device_details))
        print("--------------------------------------------------")
        print(device_details[start_index:end_index])
        print(len(device_details[start_index:end_index]))
        print("--------------------------------------------------")
        devices_for_process = device_details[start_index:end_index]
        
        # Start a new process for this batch of devices
        process = multiprocessing.Process(target=run_pipeline, args=(devices_for_process,))
        process.start()

def remove_cnts(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def load_lmdb_list():
    known_whitelist_faces1, known_whitelist_id1 = attendance_lmdb_known()
    known_blacklist_faces1, known_blacklist_id1 = attendance_lmdb_unknown()
    
    global known_whitelist_faces
    known_whitelist_faces = known_whitelist_faces1

    global known_whitelist_id
    known_whitelist_id = known_whitelist_id1
    
    global known_blacklist_faces
    known_blacklist_faces = known_blacklist_faces1

    global known_blacklist_id
    known_blacklist_id = known_blacklist_id1
    print("-------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------")
    print(len(known_whitelist_faces), len(known_blacklist_faces))
    print("-------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------")

def gen_datainfo():
    remove_cnts("./lmdb")
    load_lmdb_list()
    print("removed lmdb contents")
    mem_data = fetch_db_mem()
    for i,data in enumerate(mem_data):
        data['member'][0]['type'] = "known"
        if i == 1:
            break
    print("mem_data",mem_data)
    
    load_lmdb_fst(mem_data)
    known_whitelist_faces1, known_whitelist_id1 = attendance_lmdb_known()
    known_blacklist_faces1, known_blacklist_id1 = attendance_lmdb_unknown()
    
    global known_whitelist_faces
    known_whitelist_faces = known_whitelist_faces1

    global known_whitelist_id
    known_whitelist_id = known_whitelist_id1
    
    global known_blacklist_faces
    known_blacklist_faces = known_blacklist_faces1

    global known_blacklist_id
    known_blacklist_id = known_blacklist_id1
    return [known_whitelist_faces,known_blacklist_faces,known_whitelist_id,known_blacklist_id]
def load_lmdb_fst(mem_data):
    i = 0
    for each in mem_data:
        i = i+1
        add_member_to_lmdb(each)
        print("inserting ",each)

async def main():
    global device_details
    remove_cnts("./lmdb")
    load_lmdb_list()
    print("removed lmdb contents")
    mem_data = fetch_db_mem()
    # print(mem_data)
    
    load_lmdb_fst(mem_data)
    load_lmdb_list()

    # device_details = fetch_db()
    # print(device_details)
    temp_devs = []
    for i,each in enumerate(device_details):

        temp_devs.append(each)
        if i == 5: 
            break
            
    print(temp_devs)
    call_gstreamer(temp_devs)



if __name__ == '__main__':
    torch.multiprocessing.set_start_method('spawn', force=True)
    loop = asyncio.get_event_loop()
    try :
        # asyncio.run(main())
        loop.run_until_complete(main())
        loop.run_forever()
        watcher = PipelineWatcher(pipelines)
        watcher.loop.run()
    except RuntimeError as e:
        print("error ", e)
        print(torch.cuda.memory_summary(device=None, abbreviated=False), "cuda")
   