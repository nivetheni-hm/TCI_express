# import gi
# gi.require_version('Gst', '1.0')
# from gi.repository import Gst, GLib

# import multiprocessing as mp

# # Define the IP camera URLs
# camera_urls = [
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://test:test123456789@streams.ckdr.co.in:2554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:5554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://test:test123456789@streams.ckdr.co.in:2554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:5554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://test:test123456789@streams.ckdr.co.in:2554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
#     'rtsp://happymonk:admin123@streams.ckdr.co.in:5554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif',
# ]

# def gstreamer_bus_callback(bus, message):
#     t = message.type
#     if t == Gst.MessageType.ERROR:
#         err, debug = message.parse_error()
#         print("Error: %s" % err, debug)
#         return
#     elif t == Gst.MessageType.WARNING:
#         err, debug = message.parse_warning()
#         print("Warning: %s" % err, debug)
#     elif t == Gst.MessageType.EOS:
#         print("End-Of-Stream reached")

# def camera_process(camera_urls):
#     # Initialize GStreamer
#     Gst.init(None)

#     # Create the GStreamer pipeline for each camera
#     pipelines = []
#     for url in camera_urls:
#         pipeline = Gst.parse_launch(f'rtspsrc location={url} ! rtph264depay ! h264parse ! decodebin ! appsink name=sink emit-signals=True sync=false')
#         pipelines.append(pipeline)
        
#         # Fetch the bus for each pipeline
#         if bus is None:
#             bus = pipeline.get_bus()
#             bus.add_signal_watch()
#             bus.connect("message", gstreamer_bus_callback)

#     # Start the pipelines
#     for pipeline in pipelines:
#         pipeline.set_state(Gst.State.PLAYING)

#     # Define the callback function for new samples
#     def new_sample_callback(appsink):
#         sample = appsink.emit("pull-sample")
#         buffer = sample.get_buffer()
#         caps = sample.get_caps()
#         width = caps.get_structure(0).get_value("width")
#         height = caps.get_structure(0).get_value("height")
#         print(f"Received frame - Size: {width}x{height}")

#         return Gst.FlowReturn.OK

#     # Configure appsink callbacks
#     for pipeline in pipelines:
#         appsink = pipeline.get_by_name('sink')
#         appsink.set_property("max-buffers", 1)
#         appsink.set_property("emit-signals", True)
#         appsink.connect("new-sample", new_sample_callback)

#     # Start the main loop
#     try:
#         GLib.MainLoop().run()
#     except KeyboardInterrupt:
#         pass

#     # Stop and cleanup the pipelines
#     for pipeline in pipelines:
#         pipeline.set_state(Gst.State.NULL)

# if __name__ == '__main__':
#     # Split the camera URLs into chunks of 5 cameras
#     camera_chunks = [camera_urls[i:i+5] for i in range(0, len(camera_urls), 5)]

#     # Create a process for each camera chunk
#     processes = []
#     for chunk in camera_chunks:
#         p = mp.Process(target=camera_process, args=(chunk,))
#         processes.append(p)
#         p.start()

#     # Wait for all processes to finish
#     for p in processes:
#         p.join()


import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

import multiprocessing as mp

# Define the IP camera URLs and device IDs
camera_data = [
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 1'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 2'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:5554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 3'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 4'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 5'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 6'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 7'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:5554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 8'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 9'},
    {'url': 'rtsp://happymonk:admin123@streams.ckdr.co.in:1554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif', 'device_id': 'Camera 10'}
]

def gstreamer_bus_callback(bus, message):
    t = message.type
    if t == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print("Error: %s" % err, debug)
        return
    elif t == Gst.MessageType.WARNING:
        err, debug = message.parse_warning()
        print("Warning: %s" % err, debug)
    elif t == Gst.MessageType.EOS:
        print("End-Of-Stream reached")

def camera_process(camera_data):
    print("--------------------------------------------------")
    print(camera_data)
    print("--------------------------------------------------")

    # Initialize GStreamer
    Gst.init(None)

    # Create the GStreamer pipeline for each camera
    pipelines = []
    for data in camera_data:
        url = data['url']
        device_id = data['device_id']
        pipeline = Gst.parse_launch(f'rtspsrc location={url} ! decodebin ! appsink name=sink emit-signals=True sync=false')
        pipelines.append({'pipeline': pipeline, 'device_id': device_id})

    # Start the pipelines
    for pipeline in pipelines:
        pipeline['pipeline'].set_state(Gst.State.PLAYING)

    # Set up GStreamer bus for error handling
    bus = pipelines[0]['pipeline'].get_bus()
    bus.add_signal_watch()
    bus.connect("message", gstreamer_bus_callback)

    # Define the callback function for new samples
    def new_sample_callback(appsink, device_id):
        sample = appsink.emit("pull-sample")
        buffer = sample.get_buffer()
        caps = sample.get_caps()
        width = caps.get_structure(0).get_value("width")
        height = caps.get_structure(0).get_value("height")
        print(f"Device ID: {device_id} - Received frame - Size: {width}x{height}")

        return Gst.FlowReturn.OK

    # Configure appsink callbacks
    for pipeline in pipelines:
        appsink = pipeline['pipeline'].get_by_name('sink')
        device_id = pipeline['device_id']
        appsink.set_property("max-buffers", 1)
        appsink.set_property("emit-signals", True)
        appsink.connect("new-sample", new_sample_callback, device_id)

    # Start the main loop
    try:
        GLib.MainLoop().run()
    except KeyboardInterrupt:
        pass

    # Stop and cleanup the pipelines
    for pipeline in pipelines:
        pipeline['pipeline'].set_state(Gst.State.NULL)

if __name__ == '__main__':
    # Split the camera data into chunks of 5 cameras
    camera_chunks = [camera_data[i:i+5] for i in range(0, len(camera_data), 5)]

    # Create a process for each camera chunk
    processes = []
    for chunk in camera_chunks:
        p = mp.Process(target=camera_process, args=(chunk,))
        processes.append(p)
        p.start()

    # Wait for all processes to finish
    for p in processes:
        p.join()
