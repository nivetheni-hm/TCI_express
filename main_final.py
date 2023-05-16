import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib
import multiprocessing
import psycopg2
from os.path import join, dirname
from dotenv import load_dotenv
import os
import ast
import multiprocessing as mp
import threading
import torch
import numpy as np
from datetime import datetime
from pytz import timezone
import uuid

# importing required functions
from db_fetch import fetch_db #to fetch data from postgres
from yolo_slowfast.deep_sort.deep_sort import DeepSort # import Deepsort tracking model
from anamoly_track import trackmain # model inference part
from dev import device_details
from db_push import gif_push, gst_hls_push

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

# define constants and variables
frame_skip = {}
gif_batch = {}
known_whitelist_faces = []
known_whitelist_id = []
known_blacklist_faces = []
known_blacklist_id = []
pipeline_frm = []
pipeline_hls = []

# creation of directories for file storage
hls_path = "./Hls_output"
if os.path.exists(hls_path) is False:
    os.mkdir(hls_path)
    
gif_path = "./Gif_output"
if os.path.exists(gif_path) is False:
    os.mkdir(gif_path)

obj_model = torch.hub.load('Detection', 'custom', path='./best_yolov5.pt', source='local',force_reload=True)

def activity_trackCall(source, device_id, device_timestamp, device_data, datainfo, track_obj):

    batchId = uuid.uuid4()
    
    trackmain(
        source,
        device_data,
        device_id, 
        batchId,
        datainfo,
        obj_model,
        track_obj
        )

def numpy_creation(img_arr, device_id, device_timestamp, device_info, track_obj, skip_dict, gif_dict):
      
    # filename for gif
    video_name_gif = gif_path + '/' + str(device_id)
    if not os.path.exists(video_name_gif):
        os.makedirs(video_name_gif, exist_ok=True)
        
    path = video_name_gif + '/' + 'camera.gif'
        
    if(skip_dict[device_id] < 30):
        gif_dict[device_id].append(img_arr)
    elif(skip_dict[device_id] == 31):
        threading.Thread(target=gif_push,args=(path, device_info, gif_dict[device_id]),).start()
        
    if skip_dict[device_id] % 4 == 0:
        datainfo = [known_whitelist_faces, known_blacklist_faces, known_whitelist_id, known_blacklist_id]
        # activity_trackCall(img_arr, device_id, device_timestamp, device_info, datainfo, track_obj)
        threading.Thread(target=activity_trackCall,args=(img_arr, device_id, device_timestamp, device_info, datainfo, track_obj,)).start()

def gstreamer_bus_callback(bus, message):
    t = message.type
    if t == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print("Error: %s" % err, debug)
        return
    elif t == Gst.MessageType.WARNING:
        err, debug = message.parse_warning()
        print("Warning: %s" % err, debug)
        return
    elif t == Gst.MessageType.EOS:
        print("End-Of-Stream reached")
        return
    elif t == Gst.MessageType.STATE_CHANGED:
        old_state, new_state, pending_state = message.parse_state_changed()
        # print(f"State changed: {old_state} -> {new_state}")

def camera_process(camera_data, frameSkip, gifBatch):
    
    global pipeline_frm, pipeline_hls
    
    print("Entering Framewise and HLS Stream")
    
    # Initialize GStreamer
    Gst.init(None)
    
    # Create the GStreamer pipeline for each camera
    for data in camera_data:
        track_obj = DeepSort("./yolo_slowfast/deep_sort/deep_sort/deep/checkpoint/ckpt.t7")
        device_id = data['deviceId']
        location = data['rtsp'] # Fetching device info
        username = data['username']
        password = data['password']
        subscription = data['subscriptions']
        encode_type = data['videoEncodingInformation']
        ddns_name = data['ddns']
        
        # filename for hls
        video_name_hls = hls_path + '/' + str(device_id)
        if not os.path.exists(video_name_hls):
            os.makedirs(video_name_hls, exist_ok=True)
        
        if(ddns_name == None):
            hostname = 'hls.ckdr.co.in'
        else:
            hostname = ddns_name
        
        if((encode_type.lower()) == "h264"):
            pipeline_frm_str = Gst.parse_launch(f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! rtph264depay name=g_depay_{device_id} ! h264parse name=g_parse_{device_id} ! avdec_h264 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} sync=false')
            pipeline_hls_str = Gst.parse_launch(f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! rtph264depay name=g_depay_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}')
        if((encode_type.lower()) == "h265"):
            pipeline_frm_str = Gst.parse_launch(f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! rtph265depay name=g_depay_{device_id} ! h265parse name=g_parse_{device_id} ! avdec_h265 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} sync=false')
            pipeline_hls_str = Gst.parse_launch(f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! rtph265depay name=g_depay_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}')
        
        pipeline_frm.append({'pipeline': pipeline_frm_str, 'device_id': device_id, 'dev_data': data, 'track_obj': track_obj})
        pipeline_hls.append({'pipeline': pipeline_hls_str, 'device_id': device_id, 'ddns_name': hostname, 'file_path': video_name_hls})
    
    # Start the pipelines
    for pipeline in pipeline_frm:
        # Fetch the bus for each pipeline
        bus = pipeline['pipeline'].get_bus()
        bus.add_signal_watch()
        bus.connect("message", gstreamer_bus_callback)
        pipeline['pipeline'].set_state(Gst.State.PLAYING)
    
    # Start the pipelines
    for pipeline in pipeline_hls:
        # Fetch the bus for each pipeline
        bus = pipeline['pipeline'].get_bus()
        bus.add_signal_watch()
        bus.connect("message", gstreamer_bus_callback)
        pipeline['pipeline'].set_state(Gst.State.PLAYING)

    # Define the callback function for new samples
    def new_sample_callback(appsink, device_Id, device_Data, track_Obj):
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
        # print(f"Received frame - Size: {width}x{height}")
        frameSkip[device_Id] += 1
        datetime_ist = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
        numpy_creation(img_arr=nparray, device_id=device_Id, device_timestamp=datetime_ist , device_info=device_Data, track_obj=track_Obj, skip_dict=frameSkip, gif_dict=gif_batch)
        return Gst.FlowReturn.OK

    # Configure appsink callbacks
    for pipeline in pipeline_frm:
        deviceId = pipeline['device_id']
        appsink = pipeline['pipeline'].get_by_name(f'g_sink_{deviceId}')
        appsink.set_property("max-buffers", 1)
        appsink.set_property("emit-signals", True)
        appsink.connect("new-sample", new_sample_callback, deviceId, pipeline['dev_data'], pipeline['track_obj'])
    for pipeline in pipeline_hls:
        deviceId = pipeline['device_id']
        hostname = pipeline['ddns_name']
        video_name_hls = pipeline['file_path']
        sink = pipeline['pipeline'].get_by_name(f'h_sink_{deviceId}') # sink params
        sink.set_property('playlist-root', f'https://{hostname}/live/stream{deviceId}') # Location of the playlist to write
        sink.set_property('playlist-location', f'{video_name_hls}/{device_id}.m3u8') # Location of the playlist to write
        sink.set_property('location', f'{video_name_hls}/segment.%01d.ts') # Location of the file to write
        sink.set_property('target-duration', 10) # The target duration in seconds of a segment/file. (0 - disabled, useful for management of segment duration by the streaming server)
        sink.set_property('playlist-length', 3) # Length of HLS playlist. To allow players to conform to section 6.3.3 of the HLS specification, this should be at least 3. If set to 0, the playlist will be infinite.
        sink.set_property('max-files', 6) # Maximum number of files to keep on disk. Once the maximum is reached,old files start to be deleted to make room for new ones.
    
    # Start the main loop
    try:
        GLib.MainLoop().run()
    except KeyboardInterrupt:
        pass

    # Stop and cleanup the pipelines
    for pipeline in pipeline_frm:
        pipeline['pipeline'].set_state(Gst.State.NULL)
    for pipeline in pipeline_hls:
        pipeline['pipeline'].set_state(Gst.State.NULL)
        
def run_pipeline(devices_for_process):
    
    global frame_skip, gif_batch
    print("LENGTH OF THE DEVICES IN A PROCESS: ", len(devices_for_process))
    
    device_list = []

    for chunk in devices_for_process:
        device_dict = {}
        device_dict["deviceId"] = chunk[0]
        device_dict["tenantId"] = chunk[1]
        device_dict["urn"] = chunk[2]
        device_dict["ddns"] = chunk[3]
        device_dict["ip"] = chunk[4]
        device_dict["port"] = chunk[5]
        device_dict["videoEncodingInformation"] = chunk[6]
        device_dict["username"] = chunk[7]
        device_dict["rtsp"] = chunk[8]
        device_dict["password"] = chunk[9]
        device_dict["subscriptions"] = chunk[10]
        device_dict["lat"] = chunk[11]
        device_dict["long"] = chunk[12]
        device_list.append(device_dict)
        
    for items in device_list:
        frame_skip[items['deviceId']] = 0
        gif_batch[items['deviceId']] = []
        
    print(device_list)
    
    # mp.Process(target=gst_hls_push, args=(device_list,)).start()
    threading.Thread(target=gst_hls_push,args=(device_list,)).start()
    camera_process(device_list, frame_skip, gif_batch)

if __name__ == '__main__':

    torch.multiprocessing.set_start_method('spawn', force=True)
    
    # fetch device details
    # device_det = fetch_db()
    device_det = device_details
    
    # Split the camera data into chunks of 5 cameras
    camera_chunks = [device_det[i:i+5] for i in range(0, len(device_det), 5)]
    
    # Create a process for each camera chunk
    processes = []

    for chunk in camera_chunks:
        p = mp.Process(target=run_pipeline, args=(chunk,))
        processes.append(p)
        p.start()
        
    # Wait for all processes to finish
    for p in processes:
        p.join()