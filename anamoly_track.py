#import libraries
import numpy as np
import os,cv2,torch,random,warnings
warnings.filterwarnings("ignore",category=UserWarning)
from pathlib import Path
FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]  # yolov5 strongsort root directory
import sys
from nanoid import generate

if str(ROOT / 'Detection') not in sys.path:
    sys.path.append(str(ROOT / 'Detection'))

from pytorchvideo.transforms.functional import (
    uniform_temporal_subsample,
    short_side_scale_with_boxes,
    clip_boxes_to_image,)
from torchvision.transforms._functional_video import normalize
from pytorchvideo.data.ava import AvaLabeledVideoFramePaths
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


queue_dict = {}
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


# def save_yolopreds_tovideo(yolo_preds,id_to_ava_labels,color_map,output_video,datainfo,device_id,batchId, imgs):
#     # print("*****")
#     # print(len(imgs))
#     # print("*****")
#     if not os.path.exists("./"+device_id):
#         os.makedirs("./"+device_id)
        
#     out_path = './'+device_id+'out.jpg'
#     global frame_cnt
#     video_data = []
#     frame_data = []
#     for i, (im, pred) in enumerate(zip(yolo_preds.ims, yolo_preds.pred)):
#         frame_cnt = frame_cnt + 1
#         confff  =  yolo_preds.pandas().xyxy[0]['confidence'].tolist()
#         im=cv2.cvtColor(im,cv2.COLOR_BGR2RGB)
#         # print(len(imgs))
#         # print(i)
#         # print(inputs[i])
#         im_org= imgs[i]
#         if pred.shape[0]:
#             for j, (*box, cls, trackid, vx, vy) in enumerate(pred):
                
#                 if int(cls) != 0:
#                     ava_label = ''
#                 elif trackid in id_to_ava_labels.keys():
#                     ava_label = id_to_ava_labels[trackid].split(' ')[0]
#                 else:
#                     ava_label = 'Unknow'
#                 cd = int(trackid)
#                 detect_obj = yolo_preds.names[int(cls)]
#                 labells = ava_label
#                 # print(trackid,ava_label)
#                 text = '{} {} {}'.format(int(trackid),yolo_preds.names[int(cls)],ava_label) #[int(cls)]
#                 # crop_img = cv2.imread("/home/nivetheni/combined_pipeline/00.jpg")
#                 im = cv2.imread('/home/nivetheni/combined_pipeline/test.jpg')
#                 crop_img = save_one_box([694.0, 726.0, 1000.0, 1078.0], im, save=True)
#                 # crop_img = cv2.imread("/home/nivetheni/combined_pipeline/00.jpg")

#                 # cropimg_name = "./"+device_id+"/"+batchId+"/"+generate(size = 5)+".jpg"
#                 # print(cropimg_name)
#                 # cv2.imwrite(cropimg_name, crop_img)
                
#                 # command = 'ipfs --api={ipfs_url} add {file_path} -Q'.format(file_path=cropimg_name,ipfs_url=ipfs_url)
#                 # output = sp.getoutput(command)
#                 # cidd = output
#                 cidd = [crop_img]
#                 # os.remove(cropimg_name)
                
#                 if detect_obj == "Person":
#                     # cv2.imwrite("face_reg.jpg",im)
#                     did,track_type = find_person_type(crop_img, datainfo)
#                     # print("*****************************************")
#                     # print("*****************************************")
#                     # print("*****************************************")
#                     # print(did,track_type)
#                     # print("*****************************************")
#                     # print("*****************************************")
#                     # print("*****************************************")

#                     if len(confff) == len(pred):
#                         people = {cd: {'type': detect_obj, 'activity': labells,"confidence":yolo_preds.pandas().xyxy[0]["confidence"].tolist()[j],"did":did,"track_type":track_type,"crops":cidd}}
#                     else:
#                         people = {cd: {'type': detect_obj, 'activity': labells,"confidence":0,"did":did,"track_type":track_type,"crops":cidd}}
                
#                     text = text + " " + did
#                 else:
#                     if len(confff) == len(pred):
#                         people = {cd: {'type': detect_obj, 'activity': labells,"confidence":yolo_preds.pandas().xyxy[0]["confidence"].tolist()[j],"crops":cidd}}
#                     else:
#                         people = {cd: {'type': detect_obj, 'activity': labells,"confidence":0,"crops":cidd}}
                
                
#                 frame_data.append(people)

#                 color = color_map[int(cls)]
#                 # cv2.imwrite("before.jpg",im)
                
                
#                 im = plot_one_box(box,im,color,text)
                
#         cv2.imwrite("out.jpg",im)

#         # print("frame_data",frame_data)
#         frame_info_anamoly = anamoly_score_calculator(frame_data)

#         frame_data.clear()
#         # print(frame_data)
#         # print("frame_info_anamoly",frame_info_anamoly)
#         # anamoly_score_calculator(frame_data)
#         frame_anamoly_wgt = frame_weighted_avg(frame_info_anamoly)
#         # print(frame_info_anamoly)
#         cidd = None
#         if frame_info_anamoly != []:
#             # fullframe_name = "./"+device_id+"/"+batchId+"/"+generate(size = 5)+".jpg"
#             # cv2.imwrite(fullframe_name,im)
#             # command = 'ipfs --api={ipfs_url} add {file_path} -Q'.format(file_path=fullframe_name,ipfs_url=ipfs_url)
#             # output = sp.getoutput(command)
#             cidd = [im]
#             # os.remove(fullframe_name)
#         video_data.append({"frame_id":frame_cnt,"frame_anamoly_wgt":frame_anamoly_wgt,"detection_info":frame_info_anamoly,"cid":cidd}) 
#         output_video.write(im.astype(np.uint8))

    
    
    
#     return video_data

def get_length(filename):
    result = sp.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
        stdout=sp.PIPE,
        stderr=sp.STDOUT)
    return float(result.stdout)

# def each_cam_process():
    

def trackmain(
    inputs,
    device_id ,
    batchId,
    queue1,
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

    imgs = inputs
    yolo_preds=model(imgs, size=imsize)
    
    print(yolo_preds)
    # cv2.imwrite('output.jpg', imgs)

    # frame_cnt += frame_cnt
    
    
    # # if device_id not in queue_dict:
    # #     queue_dict[device_id] = []
    # #     queue_dict[device_id].append(yolo_preds)
    # # else:
    # #     queue_dict[device_id].append(yolo_preds)
    
    # # if len(queue_dict)>0:
    # #     for each in queue_dict:
    # #         if len(queue_dict[each])>0: 
    # #             Process(target=each_cam_process,args=queue_dict[each]) 
    
    
    # # print("DEVICE ID: ", device_id)
    # # print("LENGTH YOLO: ",len(yolo_preds))

    # deepsort_outputs=[]

    # for j in range(len(yolo_preds.pred)):
    #     temp=deepsort_update(track_obj,yolo_preds.pred[j].cpu(),yolo_preds.xywh[j][:,0:4].cpu(),yolo_preds.ims[j])
    #     if len(temp)==0:
    #         temp=np.ones((0,8))

    #     deepsort_outputs.append(temp.astype(np.float32))
    
    # yolo_preds.pred=deepsort_outputs
    
    # video_data = []
    # frame_data = []
    # for i, (im, pred) in enumerate(zip(yolo_preds.ims, yolo_preds.pred)):
        
        
    #     confff  =  yolo_preds.pandas().xyxy[0]['confidence'].tolist()
    #     im=cv2.cvtColor(im,cv2.COLOR_BGR2RGB)
    #     im_org= imgs[i]
    #     if pred.shape[0]:
    #         for j, (*box, cls, trackid, vx, vy) in enumerate(pred):
                
    #             print("DEVICE ID: ", device_id)
    #             # print("CLASS: ", cls)
    #             # print("BOX: ", *box)
    #             print("TRACK ID: ", trackid)
                
    #             cd = int(trackid)
    #             detect_obj = yolo_preds.names[int(cls)]
    #             print("DETECTION: ", detect_obj)
                
    #             # text = '{} {}'.format(int(trackid),yolo_preds.names[int(cls)]) #[int(cls)]
    #             # crop_img = save_one_box([*box], inputs, save=False)

    #             # color = color_map[int(cls)]
                
    #             # im = plot_one_box(box,inputs,color,text)
                
    #             # if os.path.exists(device_id) is False:
    #             #     os.mkdir(device_id)
                        
    #             # detect_img = "./"+device_id+"/"+str(frame_cnt)+".jpg"
    #             # # print(detect_img)
    #             # cv2.imwrite(detect_img, im)
                
    #             # print("Image saved")
                