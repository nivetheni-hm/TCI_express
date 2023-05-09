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

# importing required functions
from db_fetch import fetch_db #to fetch data from postgres
from yolo_slowfast.deep_sort.deep_sort import DeepSort # import Deepsort tracking model
from anamoly_track import trackmain # model inference part

Gst.init(None) # Initializes Gstreamer, it's variables, paths
nc_client = NATS() # global Nats declaration

# define constants and variables
frame_skip = {}
pipelines = []
device_details = []
known_whitelist_faces = []
known_whitelist_id = []
known_blacklist_faces = []
known_blacklist_id = []

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

nats_urls = os.getenv("nats")
nats_urls = ast.literal_eval(nats_urls)

# creation of directories for file storage
hls_path = "./Hls_output"
if os.path.exists(hls_path) is False:
    os.mkdir(hls_path)
    
obj_model = torch.hub.load('Detection', 'custom', path='./best_yolov5.pt', source='local',force_reload=True)

def activity_trackCall(source, device_id, device_timestamp, device_data, datainfo, track_obj):
    # global only_vehicle_batch_cnt,veh_pub
    # device_urn = device_data['urn']
    # timestampp = device_timestamp
    # lat = device_data['lat']
    # long = device_data['long']
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
        track_obj
        )

def numpy_creation(img_arr, device_id, device_timestamp, device_info, track_obj, skip_dict):
        
        # print("DEVICE ID: ", device_id)
        # print("DEVICE URN: ", device_info['urn'])
        # print("DEVICE TIMESTAMP: ", device_timestamp)
        
        # print(skip_dict)
        
        if skip_dict[device_id] % 4 == 0:
            datainfo = [known_whitelist_faces, known_blacklist_faces, known_whitelist_id, known_blacklist_id]
            activity_trackCall(img_arr, device_id, device_timestamp, device_info, datainfo, track_obj)

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
        # elif t == Gst.MessageType.STATE_CHANGED:
        #     old_state, new_state, pending_state = message.parse_state_changed()
        #     print(f"State changed: {old_state} -> {new_state}")
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
    def on_new_sample(appsink, deviceId, deviceInfo, trackObj, frameSkip): # to fetch frames from appsink
        
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
        frameSkip[deviceId] += 1
        # print("DEVICE ID: ", deviceId, "FRAME SKIP: ", frameSkip)
        # print(f"Received frame with shape {nparray.shape}")
        datetime_ist = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
        numpy_creation(img_arr=nparray, device_id=deviceId, device_timestamp=datetime_ist , device_info=deviceInfo, track_obj=trackObj, skip_dict=frameSkip)
        return Gst.FlowReturn.OK

def gst_launcher(device_data, frame_skip):
    
    print("Entering Framewise and HLS Stream")
    
    track_obj = DeepSort("./yolo_slowfast/deep_sort/deep_sort/deep/checkpoint/ckpt.t7")
    
    # print("DEVICE ID: ", device_id)
    # print("DEVICE INFO: ", device_info)
    
    device_id = device_data['deviceId']
    # location = device_data['rtsp'] # Fetching device info
    # username = device_data['username']
    # password = device_data['password']
    location = 'rtsp://216.48.184.201:8554//stream1'
    username = 'test'
    password = 'test123456789'
    encode_type = 'MP4'
    subscription = device_data['subscriptions']
    # encode_type = device_data['videoEncodingInformation']
    # ddns_name = device_data['ddns']
    ddns_name = None
    urn = device_data['urn']
    lat = device_data['lat']
    long = device_data['long']
    
    # filename for hls
    video_name_hls = hls_path + '/' + str(device_id)
    if not os.path.exists(video_name_hls):
        os.makedirs(video_name_hls, exist_ok=True)
    # print(video_name_hls)
    
    if(ddns_name == None):
        hostname = 'localhost'
    else:
        hostname = ddns_name
    
    # retry=5 retry-delay=1000 tcpserversink 
    
    if((encode_type.lower()) == "h264"):
        pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! tee name=g_t_{device_id} ! queue name=g_q_{device_id} ! rtph264depay name=g_depay_{device_id} ! h264parse name=g_parse_{device_id} ! avdec_h264 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=200 g_t_{device_id}. ! queue name=h_q_{device_id} ! rtph264depay name=h_depay_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}'
    if((encode_type.lower()) == "h265"):
        pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" user-id={username} user-pw={password} latency=50 timeout=300 drop-on-latency=true ! tee name=g_t_{device_id} ! queue name=g_q_{device_id} ! rtph265depay name=g_depay_{device_id} ! h265parse name=g_parse_{device_id} ! avdec_h265 name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True max-buffers=200 g_t_{device_id}. ! queue name=h_q_{device_id} ! rtph265depay name=h_depay_{device_id} ! mpegtsmux name=h_mux_{device_id} ! hlssink name=h_sink_{device_id}'
    if((encode_type.lower()) == "mp4"):
        pipeline_str = f'rtspsrc name=g_rtspsrc_{device_id} location={location} protocols="tcp" ! decodebin name=g_decode_{device_id} ! videoconvert name=g_videoconvert_{device_id} ! videoscale name=g_videoscale_{device_id} ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=g_sink_{device_id} emit-signals=True sync=false max-buffers=1 drop=true'
    
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
    appsink.connect("new-sample", PipelineWatcher.on_new_sample, device_id, device_data, track_obj, frame_skip)
    pipelines.append(pipeline)
    
def run_pipeline(devices_for_process):
    
    global frame_skip
    print("LENGTH OF THE DEVICES IN A PROCESS: ", len(devices_for_process))
    # print(devices_for_process)

    for device_tuple in devices_for_process:
        device_dict = {
            'deviceId': device_tuple[0],
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
        gst_launcher(device_dict, frame_skip)
        
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
        devices_for_process = device_details[start_index:end_index]
        
        # Start a new process for this batch of devices
        process = multiprocessing.Process(target=run_pipeline, args=(devices_for_process,))
        process.start()

    
async def main():
    global device_details
    # fetch device details
    device_details = fetch_db()
    # device_details = [('76b913b4-2754-4f7a-a64a-6c6de163d8e6', 'a7c4c832-249b-4a81-93da-21f56708f484', 'uuid:626d6410-d723-4dc8-o867-e943c0987dcb', None, '0.0.0.1', 1234.0, 'H264', 'test', 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'admin123', 'Activities', 26.25, 88.11), ('aaa0ec24-999f-4426-b9c8-cd2becb08c65', 'a7c4c832-249b-4a81-93da-21f56708f484', 'uuid:626d6410-d723-4dc8-o867-e943c0987dcb', None, '0.0.0.1', 1234.0, 'H264', 'happymonk', 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'admin123', 'OutDoor', 26.25, 88.11), ('c0101b52-c86b-4465-b31e-560305bb1f30', 'a7c4c832-249b-4a81-93da-21f56708f484', 'uuid:626d6410-d723-4dc8-o867-e943c0987dcb', None, '0.0.0.1', 1234.0, 'H264', 'happymonk', 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=6&subtype=0&unicast=true&proto=Onvif', 'admin123', 'Anomalies', 26.25, 88.11), ('c0101b52-c86b-4465-b31e-560305bb1f30', 'a7c4c832-249b-4a81-93da-21f56708f484', 'uuid:626d6410-d723-4dc8-o867-e943c0987dcb', None, '0.0.0.1', 1234.0, 'H264', 'happymonk', 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=6&subtype=0&unicast=true&proto=Onvif', 'admin123', 'OutDoor', 26.25, 88.11), ('de7a3898-bd42-423f-ab38-d608ef1075ac', 'a7c4c832-249b-4a81-93da-21f56708f484', 'uuid:626d6410-d723-4dc8-o867-e943c0987dcb', None, '0.0.0.1', 1234.0, 'H264', 'happymonk', 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'admin123', 'Activities', 26.25, 88.11)]
    call_gstreamer(device_details)

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
