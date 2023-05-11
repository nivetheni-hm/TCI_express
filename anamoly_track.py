#import libraries
import numpy as np
import os,cv2,torch,random,warnings
warnings.filterwarnings("ignore",category=UserWarning)
from pathlib import Path
import uuid
from pytz import timezone
import time
from datetime import datetime
import asyncio
import nats, json
FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]  # yolov5 strongsort root directory
import sys
from nanoid import generate
import threading


if str(ROOT / 'Detection') not in sys.path:
    sys.path.append(str(ROOT / 'Detection'))

from pytorchvideo.transforms.functional import (
    uniform_temporal_subsample,
    short_side_scale_with_boxes,
    clip_boxes_to_image,)
from torchvision.transforms._functional_video import normalize
from Detection.utils.plots import Annotator, colors, save_one_box


import cv2
import numpy as np
import os
from pytz import timezone 
import subprocess as sp
# from try_anamoly import anamoly_score_calculator, frame_weighted_avg
# from person_type import find_person_type
path = os.getcwd()



#.env vars loaded
import os
from os.path import join, dirname
from dotenv import load_dotenv
import ast
import gc
import os, shutil

from multiprocessing import Process

from person_type import find_person_type
from try_anamoly import anamoly_score_calculator, frame_weighted_avg
from project_1_update_ import output_func
from db_test import dbpush_activities

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


isolate_queue = {}
frame_cnt = 0

def clear_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def tensor_to_numpy(tensor):
    img = tensor.cpu().numpy().transpose((1, 2, 0))
    return img

def ava_inference_transform(clip, boxes,
    num_frames = 32, #if using slowfast_r50_detection, change this to 32, 4 for slow 
    crop_size = 640, 
    data_mean = [0.45, 0.45, 0.45], 
    data_std = [0.225, 0.225, 0.225],
    slow_fast_alpha = 4, #if using slowfast_r50_detection, change this to 4, None for slow
):
    boxes = np.array(boxes)
    roi_boxes = boxes.copy()
    clip = uniform_temporal_subsample(clip, num_frames)
    clip = clip.float()

    clip = clip / 255.0
    height, width = clip.shape[2], clip.shape[3]
    boxes = clip_boxes_to_image(boxes, height, width)
    clip, boxes = short_side_scale_with_boxes(clip,size=crop_size,boxes=boxes,)
    clip = normalize(clip,
        np.array(data_mean, dtype=np.float32),
        np.array(data_std, dtype=np.float32),) 
    boxes = clip_boxes_to_image(boxes, clip.shape[2],  clip.shape[3])
    if slow_fast_alpha is not None:
        fast_pathway = clip
        slow_pathway = torch.index_select(clip,1,
            torch.linspace(0, clip.shape[1] - 1, clip.shape[1] // slow_fast_alpha).long())
        clip = [slow_pathway, fast_pathway]
    
    return clip, torch.from_numpy(boxes), roi_boxes


def plot_one_box(x, img, color=[100,100,100], text_info="None",
                 velocity=None,thickness=1,fontsize=0.5,fontthickness=1):
    # Plots one bounding box on image img
    c1, c2 = (int(x[0]), int(x[1])), (int(x[2]), int(x[3]))
    cv2.rectangle(img, c1, c2, color, thickness, lineType=cv2.LINE_AA)
    t_size = cv2.getTextSize(text_info, cv2.FONT_HERSHEY_TRIPLEX, fontsize , fontthickness+2)[0]
    cv2.rectangle(img, c1, (c1[0] + int(t_size[0]), c1[1] + int(t_size[1]*1.45)), color, -1)
    cv2.putText(img, text_info, (c1[0], c1[1]+t_size[1]+2), 
                cv2.FONT_HERSHEY_TRIPLEX, fontsize, [255,255,255], fontthickness)
    return img


def deepsort_update(Tracker,pred,xywh,np_img):
    outputs = Tracker.update(xywh, pred[:,4:5],pred[:,5].tolist(),cv2.cvtColor(np_img,cv2.COLOR_BGR2RGB))
    return outputs

def get_length(filename):
    result = sp.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
        stdout=sp.PIPE,
        stderr=sp.STDOUT)
    return float(result.stdout)
    
async def json_publish_activity(primary):    
    nc = await nats.connect(servers=nats_urls , reconnect_time_wait= 50 ,allow_reconnect=True, connect_timeout=20, max_reconnect_attempts=60)
    js = nc.jetstream()
    JSONEncoder = json.dumps(primary)
    json_encoded = JSONEncoder.encode()
    Subject = "service.notifications"
    Stream_name = "services"
    ack = await js.publish(Subject, json_encoded)
    print(" ")
    print(f'Ack: stream={ack.stream}, sequence={ack.seq}')
    print("Activity is getting published")

def process_publish(device_id,batch_data,device_data):
    # print(device_id," ",batch_data)
    
    
    for i,each in enumerate(batch_data):
        # print("frame_id:",i+1)
        each["frame_id"] = i+1
    # print(batch_data)

    output_json = output_func(batch_data)
    
    # print(output_json)
    batchId = str(uuid.uuid4())
    output_json["tenant_id"] = device_data['tenantId']
    output_json["batchid"] = batchId
    output_json["deviceid"] = device_id
    output_json["timestamp"] = str(datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f'))
    output_json['geo']['latitude'] = device_data['lat']
    output_json['geo']['longitude'] = device_data['long']
    # geo = "testing_geo"
    # output_json["geo"] = geo
    if len(output_json["metaData"]["frameAnomalyScore"])>0:
        franaavg = sum(output_json["metaData"]["frameAnomalyScore"])/len(output_json["metaData"]["frameAnomalyScore"])
    else:
        franaavg = 0
    output_json["metaData"]["frameAnomalyScore"] = franaavg
    output_json["metaData"]['detect'] = len(output_json["metaData"]['object'])
    output_json["metaData"]['count']["peopleCount"] = len(output_json["metaData"]['object'])
    output_json["version"] = "v0.0.3"
    anamoly = ["carrying","throwing","sitting on the box","standing on the box"]
    
    if output_json['metaData']['object']:
        
        # for each in output_json['metaData']['object']:
        if len([True for each in [each["activity"] for each in output_json['metaData']['object']] if each in anamoly])>0:
            output_json["type"] = "anamoly"
            asyncio.run(json_publish_activity(primary=output_json))
            print(output_json)
            dbpush_activities(output_json)
            print("DB insert")
            
            with open("test.json", "a") as outfile:
                # data = json.load(outfile)
                # data.append(output_json)
                json.dump(output_json, outfile)
        else:
            print("DB insert")
            print(output_json)
            dbpush_activities(output_json)
            with open("test.json", "a") as outfile:
                # data = json.load(outfile)
                # data.append(output_json)
                json.dump(output_json, outfile)

    
def trackmain(
    input,
    device_data,
    device_id ,
    batchId,
    queue1,
    datainfo,
    obj_model,
    track_obj,
    device = 'cuda',
    conf = 0.5,
    classes = None,
    imsize = 640,
    iou = 0.4
):
    
    print("Starting the detection and tracking")

    global frame_cnt

    model = obj_model
    model.conf = conf
    model.iou = iou
    model.max_det = 200
    model.classes = classes
    
    color_map = [[random.randint(0, 255) for _ in range(3)] for _ in range(80)]

    img = input
    yolo_preds=model(img, size=imsize)

    frame_cnt += frame_cnt

    deepsort_outputs=[]

    for j in range(len(yolo_preds.pred)):
        temp=deepsort_update(track_obj,yolo_preds.pred[j].cpu(),yolo_preds.xywh[j][:,0:4].cpu(),yolo_preds.ims[j])
        if len(temp)==0:
            temp=np.ones((0,8))

        deepsort_outputs.append(temp.astype(np.float32))
    
    yolo_preds.pred=deepsort_outputs

    batch_data_processed = []
    frame_data = []
    im, pred = (yolo_preds.ims[0], yolo_preds.pred[0])
    confff  =  yolo_preds.pandas().xyxy[0]['confidence'].tolist()
    # print(device_id)
    if pred.shape[0]:
        for i,detection in enumerate(pred):
            *box, cls, trackid, vx, vy = detection
            # print(*box, cls, trackid, vx, vy)
            labells = yolo_preds.names[int(cls)]
            # print(labells)
            text = '{} {}'.format(int(trackid),yolo_preds.names[int(cls)]) #[int(cls)]
            crop_img = save_one_box([*box], im, save=False)
            did = ""
            track_type = "100"
            cd = int(trackid)
            cidd = [crop_img]
            if len(confff) == len(pred[0]):
                people = {cd: {'type': "Person", 'activity': labells,"confidence":yolo_preds.pandas().xyxy[0]["confidence"].tolist()[j],"did":did,"track_type":track_type,"crops":cidd}}
            else:
                people = {cd: {'type': "Person", 'activity': labells,"confidence":0,"did":did,"track_type":track_type,"crops":cidd}}
            text = text + " " + did
            frame_data.append(people)
            color = color_map[int(cls)]
            im = plot_one_box(box,im,color,text)
    # print(frame_data)

    if len(frame_data) != 0:
        frame_info_anamoly = anamoly_score_calculator(frame_data)
        # print(frame_info_anamoly)
        frame_data.clear()
        frame_anamoly_wgt = frame_weighted_avg(frame_info_anamoly)
        # print(frame_anamoly_wgt)
        cidd = None
        if frame_info_anamoly != []:
            cidd = [im]
        final_frame = {"frame_id":frame_cnt,"frame_anamoly_wgt":frame_anamoly_wgt,"detection_info":frame_info_anamoly,"cid":cidd}
        # print(final_frame)
    else:
        final_frame = {"frame_id":None,"frame_anamoly_wgt":None,"detection_info":None,"cid":None}
    if device_id in isolate_queue:
        # print("entering if ")
        isolate_queue[device_id].append(final_frame)
    else:
        # print("entering else")
        isolate_queue[device_id] = []
        isolate_queue[device_id].append(final_frame)
        
    # print([{each:len(isolate_queue[each])} for each in isolate_queue])
    for each in isolate_queue:
        
        if len(isolate_queue[each])>29:
            # print("batch length of ",device_id,":",len(isolate_queue[each]))
            batch_data = isolate_queue[each]
            isolate_queue[each] = []
            threading.Thread(target=process_publish,args = (device_id,batch_data,device_data)).start()


        # for i, (im, pred) in enumerate(zip(yolo_preds.ims, yolo_preds.pred)):
        #     confff  =  yolo_preds.pandas().xyxy[0]['confidence'].tolist()
        #     im=cv2.cvtColor(im,cv2.COLOR_BGR2RGB)
        #     im_org= img
        #     if pred.shape[0]:
        #         for j, (*box, cls, trackid, vx, vy) in enumerate(pred):
                    
        #             print("DEVICE ID: ", device_id)
        #             # print("CLASS: ", cls)
        #             # print("BOX: ", *box)
        #             print("TRACK ID: ", trackid)
                    
        #             cd = int(trackid)
        #             detect_obj = yolo_preds.names[int(cls)]
        #             labells = detect_obj
        #             # print("DETECTION: ", detect_obj)
                    
        #             text = '{} {}'.format(int(trackid),yolo_preds.names[int(cls)]) #[int(cls)]
        #             crop_img = save_one_box([*box], im, save=False)
        #             cidd = [crop_img]
        #                             # os.remove(cropimg_name)
                    
        #             # if detect_obj == "Person":
        #             did,track_type = find_person_type(crop_img, datainfo)

        #             if len(confff) == len(pred):
        #                 people = {cd: {'type': "Person", 'activity': labells,"confidence":yolo_preds.pandas().xyxy[0]["confidence"].tolist()[j],"did":did,"track_type":track_type,"crops":cidd}}
        #             else:
        #                 people = {cd: {'type': "Person", 'activity': labells,"confidence":0,"did":did,"track_type":track_type,"crops":cidd}}
                
        #             text = text + " " + did

        #             frame_data.append(people)
        #             color = color_map[int(cls)]
                    
        #             im = plot_one_box(box,im,color,text)
                    
        #         # if os.path.exists(device_id) is False:
        #         #     os.mkdir(device_id)
                        
        #         # detect_img = "./"+device_id+"/"+str(frame_cnt)+".jpg"
        #         # print(detect_img)
        #         # cv2.imwrite(detect_img, im)
                
        #         # print("Image saved")
        #     # print(frame_data)
        #     frame_info_anamoly = anamoly_score_calculator(frame_data)
        #     # print(frame_info_anamoly)
        #     frame_data.clear()
        #     frame_anamoly_wgt = frame_weighted_avg(frame_info_anamoly)
        #     # print(frame_anamoly_wgt)
        #     cidd = None
        #     if frame_info_anamoly != []:
        #         cidd = [im]

        # if device_id in isolate_queue:
        #     print("entering if ")
        #     isolate_queue[device_id].append({"frame_id":frame_cnt,"frame_anamoly_wgt":frame_anamoly_wgt,"detection_info":frame_info_anamoly,"cid":cidd})
        # else:
        #     print("entering else")
        #     isolate_queue[device_id] = []
        #     isolate_queue[device_id].append({"frame_id":frame_cnt,"frame_anamoly_wgt":frame_anamoly_wgt,"detection_info":frame_info_anamoly,"cid":cidd})
        
        # print([len(isolate_queue[each]) for each in isolate_queue])

        # for each in isolate_queue:
            
        #     if len(isolate_queue[each])>29:
        #         print("batch length of ",device_id,":",len(isolate_queue[each]))
        #         batch_data = isolate_queue[each]
        #         isolate_queue[each] = []

        #         # for every in batch_data:
        #         #     yolo_preds = every
                
        #         print(len(batch_data))
        #         print("________________________________________________________________")
        #         print("________________________________________________________________")
        #         print("________________________________________________________________")
        #         print("________________________________________________________________")
