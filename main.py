# gstreamer python bindings
import traceback
import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib

# os
import sys
import os

# concurrency and multi-processing 
import asyncio
import multiprocessing
from multiprocessing import Process, Queue, Pool
import threading
import signal

# Nats
from nats.aio.client import Client as NATS
import nats
# json
import json
# datetime
from pytz import timezone
import time
from datetime import datetime 
import imageio
import subprocess as sp
import torch
import shutil
# cv
import numpy as np
import cv2
import io
import re
import uuid

#.env vars loaded
from os.path import join, dirname
from dotenv import load_dotenv
import ast
import gc
import psutil
from nanoid import generate
from concurrent.futures import ThreadPoolExecutor
from yolo_slowfast.deep_sort.deep_sort import DeepSort

#to fetch data from postgres
from db_fetch import fetch_db
from anamoly_track import trackmain

obj_model = torch.hub.load('Detection', 'custom', path='./best_yolov5.pt', source='local',force_reload=True)
# deepsort_tracker = DeepSort("./yolo_slowfast/deep_sort/deep_sort/deep/checkpoint/ckpt.t7")
device = 'cuda'

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

nc_client = NATS() # global Nats declaration
Gst.init(sys.argv) # Initializes Gstreamer, it's variables, paths

pipeline = Gst.Pipeline()

# Define some constants
WIDTH = 1920
HEIGHT = 1080
PIXEL_SIZE = 3
processes = []
queues = []
threads = []

# creation of directories for file storage
hls_path = "./Hls_output"
gif_path = "./Gif_output"
    
if os.path.exists(hls_path) is False:
    os.mkdir(hls_path)
    
if os.path.exists(gif_path) is False:
    os.mkdir(gif_path)

# list variables
frames = []
numpy_frames = []
gif_frames = []
known_whitelist_faces = []
known_whitelist_id = []
known_blacklist_faces = []
known_blacklist_id = []
cid_unpin_cnt = 0
gif_cid_list = []
# flag variable
start_flag = False
image_count = 0
gif_batch = 0
batch_count = 0
frame_count = 0
track_type = []
veh_pub = True
only_vehicle_batch_cnt = 0
unique_device = []
frame_skip = {}

que = Queue()

def activity_trackCall(source, device_data, datainfo, track_obj):
    global only_vehicle_batch_cnt,veh_pub
    device_id = device_data[0]
    device_urn = device_data[1]
    timestampp = device_data[2]
    lat = device_data[4]
    long = device_data[5]
    queue1 = Queue()
    batchId = uuid.uuid4()
    
    trackmain(
        source, 
        device_id, 
        batchId,
        queue1, 
        datainfo,
        obj_model,
        track_obj
        )


    
def numpy_creation(img_arr, device_data, track_obj, skip_dict):
    
    device_id = device_data[0]
    device_urn = device_data[1]
    timestampp = device_data[2]
    # print(img_arr)
    if skip_dict[device_id] % 4 == 0:
        
        datainfo = [known_whitelist_faces, known_blacklist_faces, known_whitelist_id, known_blacklist_id]
        activity_trackCall(img_arr, device_data, datainfo, track_obj)
    
    
    
    # print("GETTING NUMPY CREATION")
    
    # # filename for mp4
    # video_name_gif = gif_path + '/' + str(device_id)
    # if not os.path.exists(video_name_gif):
    #     os.makedirs(video_name_gif, exist_ok=True)
        
    # timestamp = re.sub(r'[^\w\s]','',timestamp)
    
    # path = video_name_gif + '/' + str(timestamp).replace(' ','') + '.gif'
    
    # global image_count, cid_unpin_cnt, gif_batch, gif_frames
    
    # image_count += 1
    
    
    
def gst_frames(dev, track_obj, fr_skip):
    device_id, device_info = dev[0],dev[1]
    
    print("DEVICE ID: ", device_id)
    # print("DEVICE_INFO: ", device_info)
    # print("FRAME SKIP: ", fr_skip)
    
    location = device_info['rtsp'] # Fetching device info
    username = device_info['username']
    password = device_info['password']
    subscriptions = device_info['subscriptions']
    encode_type = device_info['videoEncodingInformation']
    urn = device_info['urn']
    lat = device_info['lat']
    long = device_info['long']

    print("Entering Framewise Stream")
    queue1 = {}
    data1 = []

    def new_buffer(sink, device_id, track_obj, frm_skip):  
        
        print("Entering into numpy callback")    
        
        global image_arr
        device_data = []
        device_data.append(device_id)
        device_data.append(urn)
        device_data.append(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
        device_data.append(subscriptions)
        device_data.append(lat)
        device_data.append(long)
        
        sample = sink.emit("pull-sample")
        if sample:
            print("Got a sample")
            # pipeline.get_by_name(f"g_sink_{id}")
            buffer = sample.get_buffer()
            caps = sample.get_caps()
            data = buffer.extract_dup(0, buffer.get_size())
            # Convert the bytes to a numpy array
            array = np.frombuffer(data, dtype=np.uint8)
            array = array.reshape((HEIGHT, WIDTH, PIXEL_SIZE))
            frm_skip[device_id] += 1
            # queue1[device_id] = [array]
            datetime_ist = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
            numpy_creation(img_arr=array, device_data=device_data, track_obj=track_obj, skip_dict=frm_skip)   

        return Gst.FlowReturn.OK
        # return array
    
    try:
        if((encode_type.lower()) == "h264"):
            pipeline = Gst.parse_launch('rtspsrc name=g_rtspsrc_{device_id} location={location} latency=200 protocols="tcp" user-id={username} user-pw={password} !  rtph264depay name=g_depay_{device_id} ! h264parse name=g_parse_{device_id} ! avdec_h264 name=h_decode_{device_id} ! videoconvert name=h_videoconvert_{device_id} ! videoscale name=h_videoscale_{device_id} ! videorate name=h_videorate_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} sync=false max-buffers=1 drop=true'.format(location=location, device_id=device_id, username=username, password=password))
        elif((encode_type.lower()) == "h265"):
            pipeline = Gst.parse_launch('rtspsrc name=g_rtspsrc_{device_id} location={location} latency=200 protocols="tcp" user-id={username} user-pw={password} !  rtph265depay name=g_depay_{device_id} ! h265parse name=g_parse_{device_id} ! avdec_h265 name=h_decode_{device_id} ! videoconvert name=h_videoconvert_{device_id} ! videoscale name=h_videoscale_{device_id} ! videorate name=h_videorate_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} sync=false max-buffers=1 drop=true'.format(location=location, device_id=device_id, username=username, password=password))
        elif((encode_type.lower()) == "mp4"):
            pipeline = Gst.parse_launch('rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" ! decodebin name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videorate name=h_videorate_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,framerate=15/1,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} sync=false max-buffers=1 drop=true'.format(location=location, device_id = device_id))
        if not pipeline:
            print("Not all elements could be created.")
        else:
            print("All elements are created and launched sucessfully!")
        
        # sink params
        sink = pipeline.get_by_name('g_sink_{device_id}'.format(device_id = device_id))
        
        sink.set_property("emit-signals", True)
        sink.connect("new-sample", new_buffer, device_id, track_obj, fr_skip)
        
        # GLib.MainLoop().run()
            
        bus = pipeline.get_bus()
        # allow bus to emit messages to main thread
        bus.add_signal_watch()

        # Add handler to specific signal
        bus.connect("message", on_message, loop)
        
        # Start playing
        ret = pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.SUCCESS:
            print("Able to set the pipeline to the playing state.")
        if ret == Gst.StateChangeReturn.FAILURE:
            print("Unable to set the pipeline to the playing state.")  
    
    except TypeError as e:
        print(TypeError," gstreamer Gif error >> ", e)  
             
#@profile
def call_gstreamer(device_data):
    print("Got device info from DB")
    devs = []
    global frame_skip
    for i,key in enumerate(device_data):
        device_data[key]['rtsp'] = 'rtsp://216.48.184.201:8554//stream1'
        device_data[key]['videoEncodingInformation'] = 'MP4'
        devs.append(key)
        devs.append(device_data[key])
        frame_skip[key] = 0
        track_obj = DeepSort("./yolo_slowfast/deep_sort/deep_sort/deep/checkpoint/ckpt.t7")
        gst_frames(devs, track_obj, frame_skip)
        devs.clear()

def on_message(bus: Gst.Bus, message: Gst.Message, loop: GLib.MainLoop):
    mtype = message.type
    """
        Gstreamer Message Types and how to parse
        https://lazka.github.io/pgi-docs/Gst-1.0/flags.html#Gst.MessageType
    """
    if mtype == Gst.MessageType.EOS:
        print("End of stream")
        loop.quit()

    elif mtype == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print(("Error received from element %s: %s" % (
            message.src.get_name(), err)))
        print(("Debugging information: %s" % debug))
        loop.quit()

    elif mtype == Gst.MessageType.STATE_CHANGED:
        if isinstance(message.src, Gst.Pipeline):
            old_state, new_state, pending_state = message.parse_state_changed()
            print(("Pipeline state changed from %s to %s." %
            (old_state.value_nick, new_state.value_nick)))
    return True

async def main():
    
    device_data = fetch_db()
    call_gstreamer(device_data)
    
    
    # try:
    #     # pipeline = Gst.parse_launch('fakesrc ! queue ! fakesink')
        
    #     # # Init GObject loop to handle Gstreamer Bus Events
    #     # loop = GLib.MainLoop()

    #     # bus = pipeline.get_bus()
    #     # # allow bus to emit messages to main thread
    #     # bus.add_signal_watch()

    #     # # Add handler to specific signal
    #     # bus.connect("message", on_message, loop)

    #     # # Start pipeline
    #     # pipeline.set_state(Gst.State.PLAYING)

    #     device_data = fetch_db()
    #     # # print(device_data)
    #     call_gstreamer(device_data)
        
    #     # loop.run()
        
    # except Exception as e:
    #     loop.quit()
    #     pipeline.set_state(Gst.State.NULL)
    #     del pipeline
    #     # await nc_client.close() # Close NATS connection
    #     # print("Nats encountered an error: \n", e)


if __name__ == '__main__':
    torch.multiprocessing.set_start_method('spawn', force=True)
    loop = asyncio.get_event_loop()
    try :
        # asyncio.run(main())
        loop.run_until_complete(main())
        loop.run_forever()
    except RuntimeError as e:
        print("error ", e)
        print(torch.cuda.memory_summary(device=None, abbreviated=False), "cuda")