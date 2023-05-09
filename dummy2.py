import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib
import numpy as np
import multiprocessing as mp

Gst.init(None)

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
            print(f"Error: {err}", debug)
            self.restart_pipeline(pipeline)
        elif t == Gst.MessageType.WARNING:
            err, debug = message.parse_warning()
            print(f"Warning: {err}", debug)
        elif t == Gst.MessageType.EOS:
            print("End of stream")
            self.restart_pipeline(pipeline)
        elif t == Gst.MessageType.STATE_CHANGED:
            old_state, new_state, pending_state = message.parse_state_changed()
            print(f"State changed: {old_state} -> {new_state}")
        elif t == Gst.MessageType.STREAM_START:
            print("Stream started")
        elif t == Gst.MessageType.STREAM_STATUS:
            status_type, owner = message.parse_stream_status()
            print(f"Stream status: {status_type} - {owner}")
        elif t == Gst.MessageType.TAG:
            taglist = message.parse_tag()
            print(f"Tag: {taglist}")
        elif t == Gst.MessageType.ASYNC_DONE:
            print("Async done")
        else:
            print(f"Unknown message: {t}")

    def restart_pipeline(self, pipeline):
        pipeline.set_state(Gst.State.NULL)
        pipeline.set_state(Gst.State.PLAYING)
        for appsink in pipeline.iterate_elements().filter(Gst.ElementFactory.make('appsink')):
            appsink.connect("new-sample", self.on_new_sample, pipeline)

    @staticmethod
    def on_new_sample(appsink, pipeline):
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
        print(f"Received frame with shape {nparray.shape}")

def process_pipeline(pipelines):
    watcher = PipelineWatcher(pipelines)
    watcher.loop.run()

if __name__ == '__main__':
    devices = 5
    processes = []
    pipelines = []

    for i in range(1, devices):
        pipeline_str = f"rtspsrc location=rtsp://216.48.184.201:8554//stream{i} protocols=tcp latency=50 timeout=300 drop-on-latency=true ! decodebin ! videoconvert ! videoscale ! video/x-raw,format=BGR,width=1920,height=1080,pixel-aspect-ratio=1/1,bpp=24 ! appsink name=sink{i} emit-signals=True max-buffers=200"
        pipeline = Gst.parse_launch(pipeline_str)
        pipelines.append(pipeline)

    # Divide devices into groups of 5
    groups = [pipelines[i:i+5] for i in range(0, devices, 5)]

    for group in groups:
        p = mp.Process(target=process_pipeline, args=(group,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
